# ⚡ Spark Performance Tuning & Lakehouse Pipeline

> **Technical Impact:** **50% reduction in processing time** through cluster tuning and query optimization.
> **Business Impact:** End-to-end automation that **reduced manual KPI consolidation effort by 90%**.

---

## 🎯 Executive Summary
This project showcases a **Lakehouse architecture (Bronze → Silver → Gold)** implementation using PySpark. The core value of this repository lies in the application of advanced **Query Tuning** and **Partitioning Management** techniques, ensuring high performance and scalability in Big Data environments.

---

## 🏗️ Architecture & Data Strategy

| Layer | Format | Performance Technique Applied |
| :--- | :--- | :--- |
| **Bronze** | Delta | **Explicit Schemas:** Prevents inference overhead and ensures data contract. |
| **Silver** | Delta | **Z-Order Clustering:** Optimized I/O for region and date-based filtering. |
| **Gold** | Delta | **Broadcast Joins & Strategic Caching:** Minimized data shuffling for dimension tables. |

---

## 🚀 Performance Engineering Highlights (Mid-Level)

Unlike standard pipelines, this project was engineered to solve the **Small Files Problem** and network bottlenecks:

* **Automated Broadcast Joins:** Tuned `autoBroadcastJoinThreshold` to accelerate joins between high-volume sales facts and category dimensions.
* **Efficient Window Functions:** Implemented `rank()` and `lag()` with memory partitioning to calculate MoM growth without driver overhead.
* **Smart Caching Strategy:** Strategic use of `.cache()` in the Silver layer for reusability across multiple Gold aggregations, significantly cutting disk I/O.
* **Shuffle Management:** Fine-tuned `spark.sql.shuffle.partitions` to balance parallelism according to dataset volume.

---

## 📂 Technical Structure
* `notebooks/`: Processing scripts containing transformation and aggregation logic.
* `scripts/spark_utils.py`: Maintenance utility functions (**OPTIMIZE/VACUUM**).
* `docs/architecture.png`: Visual data flow diagram.

------------------------------------------------------------------------------------------------------------------------------------
Versão Português

# ⚡ Spark Performance Tuning & Lakehouse Pipeline

> **Impacto Técnico:** Redução de **50% no tempo de processamento** através de tuning de cluster e otimização de queries.
> **Impacto de Negócio:** Automação end-to-end que **reduziu em 90% o esforço manual** de consolidação de KPIs logísticos e comerciais.

---

## 🎯 Resumo Executivo
Este projeto demonstra a implementação de uma arquitetura **Lakehouse (Bronze → Silver → Gold)** utilizando PySpark. O diferencial desta entrega não é apenas a movimentação dos dados, mas a aplicação de técnicas avançadas de **Query Tuning** e **Gestão de Particionamento** para garantir escalabilidade em ambientes de Big Data.

---

## 🏗️ Arquitetura e Estratégia de Dados

| Camada | Formato | Técnica de Performance Aplicada |
| :--- | :--- | :--- |
| **Bronze** | Delta | **Schema Enforcement:** Previne o overhead de inferência e garante integridade. |
| **Silver** | Delta | **Z-Order Clustering:** Otimização de I/O em filtros de região e data. |
| **Gold** | Delta | **Broadcast Joins & Caching:** Eliminação de shuffles desnecessários em tabelas de dimensão. |

---

## 🚀 Destaques de Performance (Nível Pleno)

Diferente de pipelines convencionais, este projeto foi otimizado para lidar com o **Small Files Problem** e gargalos de rede:

* **Broadcast Join Automático:** Configuração de `autoBroadcastJoinThreshold` para acelerar joins entre fatos de vendas e dimensões de categorias.
* **Window Functions Otimizadas:** Uso de `rank()` e `lag()` com particionamento de memória para calcular crescimento MoM sem sobrecarregar o driver.
* **Estratégia de Cache:** Implementação de `.cache()` estratégico na camada Silver para reaproveitamento de dados em múltiplas agregações Gold, reduzindo leituras de disco.
* **Gerenciamento de Shuffle:** Ajuste fino de `spark.sql.shuffle.partitions` para equilibrar o paralelismo conforme o volume do dataset.

---

## 📂 Estrutura Técnica
* `notebooks/`: Scripts de processamento com lógica de transformação e agregação.
* `scripts/spark_utils.py`: Funções utilitárias para manutenção de tabelas (**OPTIMIZE/VACUUM**).
* `docs/architecture.png`: Diagrama visual do fluxo de dados.

---

## 👤 Author
**Francian Rodrigues Santos** - Data Scientist & SAS Developer.
[LinkedIn](https://www.linkedin.com/in/francianrodrigues) | [GitHub](https://github.com/FrancianRod)
