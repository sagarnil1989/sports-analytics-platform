---
name: Infrastructure changes always via Terraform
description: User wants all Azure infrastructure changes done through Terraform only, never via az CLI workarounds
type: feedback
---

Always make infrastructure changes through Terraform. Never suggest `az` CLI commands as a fix or workaround for infrastructure issues (permissions, access policies, secrets, role assignments, etc.). If something is broken, fix the Terraform code and run `terraform apply`.

**Why:** User wants Terraform as the single source of truth for all infrastructure. Ad-hoc CLI commands create drift between actual state and Terraform state.

**How to apply:** When diagnosing infrastructure errors (permissions, missing resources, misconfiguration), update the relevant `.tf` file and instruct the user to run `terraform apply`. Do not offer `az` CLI as an alternative or shortcut.
