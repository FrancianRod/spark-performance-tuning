# Diagrama de Arquitetura

O arquivo representa o fluxo completo do pipeline:

```
Fontes (CSV / JSON)
    ↓
Bronze — Raw ingestion com schema enforcement e metadados de auditoria
    ↓
Silver — Limpeza, deduplicação, normalização de tipos e Data Quality Gate
    ↓
Gold  — 4 tabelas agregadas (KPIs de vendas + logística)
    ↓
Power BI — Dashboards executivos
```
