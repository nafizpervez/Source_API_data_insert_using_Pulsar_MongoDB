FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY . .
WORKDIR /src/src/SourceIngestor.Worker

# Correct order: restore -> build -> publish
RUN dotnet restore
RUN dotnet build -c Release --no-restore
RUN dotnet publish -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app

# tzdata is required for TimeZoneInfo conversions on Linux containers
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

# Optional but recommended: set container TZ (you can override in docker-compose env)
ENV TZ=Asia/Dhaka

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "SourceIngestor.Worker.dll"]