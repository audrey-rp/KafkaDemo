#!/usr/bin/env pwsh
# Setup Kafka topics for the demo

param(
    [switch]$Reset
)

$ErrorActionPreference = "Stop"

function Wait-ForKafka {
    param(
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 1
    )

    Write-Host "Checking Kafka readiness..." -ForegroundColor Yellow

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        if (-not (docker ps --format "{{.Names}}" | Select-String -SimpleMatch "kafka")) {
            Start-Sleep -Seconds $DelaySeconds
            continue
        }

        docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Kafka is ready." -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Kafka did not become ready within $MaxAttempts seconds. Run 'docker compose up -d' and try again."
}

function Wait-ForTopicDeletion {
    param(
        [Parameter(Mandatory = $true)][string]$TopicName,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 1
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $existingTopics = docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
        if (-not ($existingTopics -split "`r?`n" | Where-Object { $_ -eq $TopicName })) {
            return
        }

        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Topic '$TopicName' was not deleted within $MaxAttempts seconds."
}

$topics = @(
    @{ name = "demo-topic"; partitions = 3; replicationFactor = 1 }
)

Wait-ForKafka

foreach ($topic in $topics) {
    if ($Reset) {
        Write-Host "Resetting topic: $($topic.name)..." -ForegroundColor Yellow
        docker exec kafka kafka-topics --delete `
            --topic $($topic.name) `
            --bootstrap-server localhost:9092

        Wait-ForTopicDeletion -TopicName $topic.name
        Write-Host "Deleted topic: $($topic.name)" -ForegroundColor Green
    }

    Write-Host "Creating topic: $($topic.name)..." -ForegroundColor Cyan
    docker exec kafka kafka-topics --create `
        --topic $($topic.name) `
        --bootstrap-server localhost:9092 `
        --partitions $topic.partitions `
        --replication-factor $topic.replicationFactor `
        --if-not-exists
    Write-Host "Done." -ForegroundColor Green
}

Write-Host "`nTopic setup complete!" -ForegroundColor Green
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
