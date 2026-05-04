// Centralised constants. Override via workflow_settings.yaml vars.
const PROJECT_ID = dataform.projectConfig.vars.ga4_project || "your-gcp-project";
const GA4_DATASET = dataform.projectConfig.vars.ga4_dataset || "analytics_123456789";
const LOOKBACK_DAYS = dataform.projectConfig.vars.lookback_days || 3;
const MP_LOOKBACK_DAYS = dataform.projectConfig.vars.mp_lookback_days || 60;

module.exports = { PROJECT_ID, GA4_DATASET, LOOKBACK_DAYS, MP_LOOKBACK_DAYS };
