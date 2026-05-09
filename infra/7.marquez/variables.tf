variable "marquez_db_password" {
  description = "Password for the marquez PostgreSQL user on psql-cricwebsite. Must match the server admin password."
  type        = string
  sensitive   = true
}

variable "dockerhub_username" {
  description = "Docker Hub username — used to authenticate ACI image pulls and avoid rate limiting."
  type        = string
}

variable "dockerhub_token" {
  description = "Docker Hub access token (not password) — create at hub.docker.com → Account Settings → Security."
  type        = string
  sensitive   = true
}
