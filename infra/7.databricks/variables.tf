variable "bets_api_token" {
  description = "BetsAPI authentication token — pass via TF_VAR_bets_api_token in CI or locally."
  type        = string
  sensitive   = true
}
