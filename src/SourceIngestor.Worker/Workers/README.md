# Workers – Validation, Typing, De-duplication Rules

This folder contains background workers that process Source API data through MongoDB collections, applying validation, optional type coercion, and duplication detection.

---

## Pipeline Overview (High Level)

1. **MongoWriter**

   - Consumes batch messages from Pulsar (`PostsBatchTopic`)
   - Writes a single “latest snapshot” document into Mongo collection **`Source_Data`**
   - Structure includes:
     - `_id` (JobId as Mongo ObjectId)
     - `sourceUrl`, `fetchedAtLocalText`, `timeZoneId`
     - `typeMap` (from source)
     - `payload` (array of items)
2. **ValidationProcessor**

   - Reads from **`Source_Data`**
   - Produces:
     - **`Valid_Data`** (validated + deduped items)
     - **`Invalid_Data`** (invalid items)
     - **`Duplicated_Data`** (duplicates detected by configured key)

> Note: Current implementation keeps only the **latest snapshot** by clearing output collections before inserting updated results.

---

## Output Collections and What Goes Where

### 1) Valid_Data

A record is placed into **Valid_Data.payload** if:

- It passes all **required-field validation** rules (see below)
- It passes **dedup-key generation**
- It is the **first occurrence** of its dedup key inside the current batch

Each valid item also includes the computed key field (configured name, default: `Candidate_Key_combination`).

### 2) Invalid_Data

A record is placed into **Invalid_Data.payload** if it fails ANY required rule, including:

- Missing/Null/Empty required numeric fields (`userId`, `id`)
- Missing/Null required text fields (`title`, `body`)
- Dedup key cannot be computed (because key fields missing/null/empty)

### 3) Duplicated_Data

A record is placed into **Duplicated_Data.payload** if:

- It is otherwise valid (all required fields OK, key computed successfully)
- Its dedup key already appeared earlier in the same batch

Duplicates are identified by either:

- Primary key mode (single field)
- Candidate key mode (multiple fields combined)

---

## Required Field Validation Rules

### A) userId (required)

Validated using `TryGetInt(doc, "userId", ...)`

**Valid inputs**:

- `8` → 8
- `8.6` → floor → 8
- `"8"` → 8
- `"8.0"` → floor → 8
- `"8.6"` → floor → 8

**Invalid inputs**:

- `null` → invalid (`missing_or_null`)
- `""` or `"   "` → invalid (`string_empty`)
- `"abc"` → invalid (`string_not_numeric`)
- `NaN` / `Infinity` → invalid
- Out of `int32` range → invalid

### B) id (required)

Validated using `TryGetInt(doc, "id", ...)`

**Valid inputs**:

- `76` → 76
- `76.6` → floor → 76
- `"76"` → 76
- `"76.0"` → floor → 76
- `"76.6"` → floor → 76

**Invalid inputs**:

- `null` → invalid (`missing_or_null`)
- `""` or `"   "` → invalid (`string_empty`)
- `"xyz"` → invalid (`string_not_numeric`)
- `NaN` / `Infinity` → invalid
- Out of `int32` range → invalid

---

## Title and Body Resulting Behavior (Current Rules)

### C) title (required, non-null)

Validation uses `TryGetRequiredNonNullField(doc, "title", ...)` plus coercion with `GetStringCoerce(doc, "title")`.

**Rules**:

- `title` must exist (key present)
- `title` must NOT be `null`
- Empty string is allowed
- Non-string values are coerced to string for valid records

**Examples**:

- `"title": "hello"` → ✅ valid, stored as `"hello"`
- `"title": ""` → ✅ valid (empty allowed), stored as `""`
- `"title": "   "` → ✅ valid (whitespace allowed), stored as `"   "` (trim is NOT enforced)
- `"title": 123` → ✅ valid, stored as `"123"` (coerced via `.ToString()`)
- `"title": true` → ✅ valid, stored as `"true"`
- `"title": null` → ❌ invalid (`null_not_allowed`)
- missing `title` key → ❌ invalid (`missing`)

### D) body (required, non-null)

Validation uses `TryGetRequiredNonNullField(doc, "body", ...)` plus coercion with `GetStringOrNull(doc, "body")`.

**Rules**:

- `body` must exist (key present)
- `body` must NOT be `null`
- Empty string is allowed
- Non-string values are coerced to string for valid records

**Examples**:

- `"body": "text"` → ✅ valid, stored as `"text"`
- `"body": ""` → ✅ valid (empty allowed)
- `"body": 456` → ✅ valid, stored as `"456"` (coerced)
- `"body": null` → ❌ invalid (`null_not_allowed`)
- missing `body` key → ❌ invalid (`missing`)

> If you want to treat empty string for `title` or `body` as invalid, the validator must be tightened to reject `""` / whitespace (not currently enforced).

---

## De-duplication Rules

De-duplication is controlled by `appsettings.json` under the `Duplication` section.

### Config

- `Mode`: `CandidateKey` OR `PrimaryKey`
- `CandidateKeyFields`: list of field names (e.g., `["userId","id"]`)
- `PrimaryKeyField`: single field name (e.g., `"id_PK"`)
- `KeyJoiner`: string used to join candidate key parts (e.g., `"|"`)
- `ComputedKeyFieldName`: field added into each output item (default: `"Candidate_Key_combination"`)

### Candidate Key Mode

Candidate key is built by:

1. Reading each configured field value from the raw record
2. Normalizing each value to a string (numbers -> string)
3. Joining the parts using `KeyJoiner`

**Example configuration**:

```json
"Duplication": {
  "Mode": "CandidateKey",
  "CandidateKeyFields": ["userId", "id"],
  "KeyJoiner": "|",
  "ComputedKeyFieldName": "Candidate_Key_combination"
}
```

## Example computed key

- `userId = 8`, `id = 76` → computed key = `8|76`

## Duplicate decision

- First time `8|76` appears → goes to **Valid_Data**
- Next time `8|76` appears → goes to **Duplicated_Data**

---

## Primary Key Mode

Primary key is built from a single configured field:

- `PrimaryKeyField = "id_PK"`
- Key = normalized value of `doc["id_PK"]`

---

## Data Type Validation / Coercion Notes

### Numeric fields (`userId`, `id`)
**Accept:**
- `int32`, `int64`, `double`, `decimal128`, numeric strings

**Floor rule (for any decimal numeric):**
- `"76.6"` → `76`
- `76.6` → `76`

**Reject:**
- `null`
- empty string (`""` or whitespace)
- non-numeric string (e.g., `"abc"`)
- `NaN` / `Infinity`
- out-of-range (outside `int32`)

### Text fields (`title`, `body`)
- Must exist and be **non-null**
- Empty string is allowed
- Non-string values are coerced into string
```
