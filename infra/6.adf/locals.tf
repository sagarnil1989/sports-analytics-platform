locals {
  config   = jsondecode(file("../../env/local.config.json"))
  adf_name = "adf-${local.config.project}"

  # Marquez OpenLineage endpoint — populated in env/local.config.json after deploying infra/7.marquez.
  # Leave as empty string to skip lineage emission; pipelines still work normally.
  marquez_lineage_url = try(local.config.marquez_api_url, "")

  # ADF expression strings for OpenLineage events.
  # pipeline().RunId is an ADF built-in — gives a unique UUID per pipeline run so
  # Marquez can correlate the START and COMPLETE events into one job run.

  ol_start_nightly = "@concat('{\"eventType\":\"START\",\"eventTime\":\"',utcNow(),'\",\"producer\":\"adf-${local.config.project}\",\"schemaURL\":\"https://openlineage.io/spec/1-0-5/OpenLineage.json\",\"run\":{\"runId\":\"',pipeline().RunId,'\"},\"job\":{\"namespace\":\"adf-${local.config.project}\",\"name\":\"nightly_batch_silver\"},\"inputs\":[{\"namespace\":\"gold\",\"name\":\"cricket/innings_tracker/index.json\"}],\"outputs\":[{\"namespace\":\"silver\",\"name\":\"cricket/event_id=star/innings_accumulator\"}]}')"

  ol_complete_nightly = "@concat('{\"eventType\":\"COMPLETE\",\"eventTime\":\"',utcNow(),'\",\"producer\":\"adf-${local.config.project}\",\"schemaURL\":\"https://openlineage.io/spec/1-0-5/OpenLineage.json\",\"run\":{\"runId\":\"',pipeline().RunId,'\"},\"job\":{\"namespace\":\"adf-${local.config.project}\",\"name\":\"nightly_batch_silver\"},\"inputs\":[{\"namespace\":\"gold\",\"name\":\"cricket/innings_tracker/index.json\"}],\"outputs\":[{\"namespace\":\"silver\",\"name\":\"cricket/event_id=star/innings_accumulator\"}]}')"

  ol_start_reprocess = "@concat('{\"eventType\":\"START\",\"eventTime\":\"',utcNow(),'\",\"producer\":\"adf-${local.config.project}\",\"schemaURL\":\"https://openlineage.io/spec/1-0-5/OpenLineage.json\",\"run\":{\"runId\":\"',pipeline().RunId,'\"},\"job\":{\"namespace\":\"adf-${local.config.project}\",\"name\":\"batch_silver_reprocess\"},\"inputs\":[{\"namespace\":\"silver\",\"name\":\"cricket/event_id=',pipeline().parameters.event_id,'/innings_accumulator\"}],\"outputs\":[{\"namespace\":\"gold\",\"name\":\"cricket/innings_tracker/event_id=',pipeline().parameters.event_id,'/innings_1_from_silver.json\"}]}')"

  ol_complete_reprocess = "@concat('{\"eventType\":\"COMPLETE\",\"eventTime\":\"',utcNow(),'\",\"producer\":\"adf-${local.config.project}\",\"schemaURL\":\"https://openlineage.io/spec/1-0-5/OpenLineage.json\",\"run\":{\"runId\":\"',pipeline().RunId,'\"},\"job\":{\"namespace\":\"adf-${local.config.project}\",\"name\":\"batch_silver_reprocess\"},\"inputs\":[{\"namespace\":\"silver\",\"name\":\"cricket/event_id=',pipeline().parameters.event_id,'/innings_accumulator\"}],\"outputs\":[{\"namespace\":\"gold\",\"name\":\"cricket/innings_tracker/event_id=',pipeline().parameters.event_id,'/innings_1_from_silver.json\"}]}')"
}
