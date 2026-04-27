"""
generate_sample_data.py
=======================
Gera dados de amostra realistas para o spark-lakehouse-pipeline.

Uso:
    python scripts/generate_sample_data.py

Saída:
    data/raw/sales_data.csv
    data/raw/logistics_data.json
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
random.seed(42)

OUTPUT_DIR_CSV  = "data/raw"
OUTPUT_DIR_JSON = "data/raw"
N_SALES         = 5_000
N_LOGISTICS     = 4_200

os.makedirs(OUTPUT_DIR_CSV, exist_ok=True)

# ---------------------------------------------------------------------------
# Dados de referência (simulam domínios de negócio reais)
# ---------------------------------------------------------------------------
REGIONS     = ["NORDESTE", "SUDESTE", "SUL", "CENTRO-OESTE", "NORTE"]
CATEGORIES  = ["Eletronicos", "Vestuario", "Alimentos", "Moveis", "Higiene", "Esportes"]
CARRIERS    = ["Correios", "Jadlog", "Total Express", "Azul Cargo", "Sequoia"]
STATUSES    = ["ENTREGUE", "ENTREGUE", "ENTREGUE", "EM_TRANSITO", "ATRASADO"]  # distribuição realista

def random_date(start_days_ago: int = 365) -> str:
    base = datetime.now() - timedelta(days=random.randint(0, start_days_ago))
    return base.strftime("%Y-%m-%d")

def random_price(min_val: float, max_val: float) -> str:
    return f"${random.uniform(min_val, max_val):.2f}"

# ---------------------------------------------------------------------------
# Gera sales_data.csv
# ---------------------------------------------------------------------------
def generate_sales():
    headers = [
        "order_id", "order_date", "customer_id", "product_id",
        "product_category", "region", "sales_amount",
        "quantity", "discount", "salesperson_id"
    ]

    rows = []
    for i in range(1, N_SALES + 1):
        # Injeta ~2% de duplicatas intencionais para testar deduplicação na Silver
        order_id = f"ORD-{i:05d}" if random.random() > 0.02 else f"ORD-{random.randint(1, i):05d}"

        rows.append({
            "order_id":         order_id,
            "order_date":       random_date(365),
            "customer_id":      f"CLI-{random.randint(1, 800):04d}",
            "product_id":       f"PRD-{random.randint(1, 200):04d}",
            "product_category": random.choice(CATEGORIES),
            "region":           random.choice(REGIONS),
            "sales_amount":     random_price(50, 8000),
            "quantity":         str(random.randint(1, 20)),
            "discount":         f"{random.choice([0, 0, 5, 10, 15, 20])}%",
            "salesperson_id":   f"VEN-{random.randint(1, 30):03d}",
        })

        # Injeta ~1% de registros com sales_amount nulo para testar filtro da Silver
        if random.random() < 0.01:
            rows[-1]["sales_amount"] = ""

    path = os.path.join(OUTPUT_DIR_CSV, "sales_data.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] sales_data.csv gerado: {len(rows)} registros → {path}")


# ---------------------------------------------------------------------------
# Gera logistics_data.json
# ---------------------------------------------------------------------------
def generate_logistics():
    records = []
    for i in range(1, N_LOGISTICS + 1):
        dispatch = datetime.now() - timedelta(days=random.randint(10, 400))
        lead_time = random.randint(1, 20)
        delivery  = dispatch + timedelta(days=lead_time)

        # ~5% dos registros sem data de entrega (pedidos em trânsito)
        delivery_str = delivery.strftime("%Y-%m-%d") if random.random() > 0.05 else None

        records.append({
            "shipment_id":      f"SHP-{i:06d}",
            "order_id":         f"ORD-{random.randint(1, N_SALES):05d}",
            "dispatch_date":    dispatch.strftime("%Y-%m-%d"),
            "delivery_date":    delivery_str,
            "carrier":          random.choice(CARRIERS),
            "origin_city":      random.choice(["São Paulo", "Recife", "Manaus", "Curitiba", "Brasília"]),
            "destination_city": random.choice(["Rio de Janeiro", "Fortaleza", "Porto Alegre", "Salvador", "Goiânia"]),
            "freight_cost":     round(random.uniform(8, 350), 2),
            "status":           random.choice(STATUSES) if delivery_str else "EM_TRANSITO",
        })

    path = os.path.join(OUTPUT_DIR_JSON, "logistics_data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"[OK] logistics_data.json gerado: {len(records)} registros → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Gerando dados de amostra para o spark-lakehouse-pipeline...")
    generate_sales()
    generate_logistics()
    print("\nDados prontos! Execute os notebooks na ordem:")
    print("  1. notebooks/01_ingestion_bronze.py")
    print("  2. notebooks/02_transformation_silver.py")
    print("  3. notebooks/03_aggregation_gold.py")
