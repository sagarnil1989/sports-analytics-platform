# Cost and Purpose

## Resources

| Resource | Purpose | Approx monthly MVP cost |
|---|---|---:|
| Resource Group | Logical container | 0 |
| ADLS Gen2 Storage Account | Bronze/Silver/Gold lake storage | 2-5 EUR |
| Function Storage Account | Required by Azure Functions runtime | 1 EUR |
| Storage Queue | Parser queue / decoupling | near 0 |
| Key Vault | Store API keys and secrets | ~1 EUR |
| Log Analytics + Application Insights | Monitoring and errors | 3-10 EUR |
| Azure SQL Database Basic | Serving database for website/Power BI | ~5 EUR |
| Azure Functions Consumption | Poller/parser/API compute | often near 0 at MVP scale |
| Static Web App Free | Frontend hosting | 0 |

Expected MVP total: roughly 15-30 EUR/month, depending mostly on SQL tier and logging volume.
