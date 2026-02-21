namespace SourceIngestor.Worker.Options;

public enum DuplicationMode
{
    PrimaryKey = 0,
    CandidateKey = 1
}

public sealed class DuplicationOptions
{
    // "PrimaryKey" or "CandidateKey"
    public DuplicationMode Mode { get; set; } = DuplicationMode.CandidateKey;

    // Used only when Mode=PrimaryKey (single field)
    public string PrimaryKeyField { get; set; } = "";

    // Used only when Mode=CandidateKey (multi-field)
    public string[] CandidateKeyFields { get; set; } = Array.Empty<string>();

    // The field name to store computed key in each item
    // Your example: "Candidate_Key_combination"
    public string ComputedKeyFieldName { get; set; } = "Candidate_Key_combination";

    // Joiner used when combining CandidateKeyFields.
    // Your example expects "11" and "12" => joiner should be "".
    // For safer keys you can change to "|".
    public string KeyJoiner { get; set; } = "";
}