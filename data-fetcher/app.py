from flask import Flask, jsonify, request
import pandas as pd
from datetime import datetime
from pathlib import Path


app = Flask(__name__)

# load once (fast)
sales_df = pd.read_csv("data/fact_sales.csv")

@app.route("/sales", methods=["GET"])
def get_sales():
    date_param = request.args.get("date")

    if date_param:
        date_id = int(date_param)
    else:
        date_id = int(datetime.today().strftime("%Y%m%d"))

    result = sales_df[sales_df["date_id"] == date_id]

    return jsonify(result.to_dict(orient="records"))

@app.route("/dimension_tables", methods=["GET"])
def dimension_tables():
    dimension_dfs = {
        "dim_date": pd.read_csv("data/dim_date.csv"),
        "dim_product": pd.read_csv("data/dim_product.csv"),
        "dim_store": pd.read_csv("data/dim_store.csv"),
        "dim_customer": pd.read_csv("data/dim_customer.csv"),
    }

    result = {
        "tables": list(dimension_dfs.keys()),
        "data": {
            name: df.to_dict(orient="records")
            for name, df in dimension_dfs.items()
        },
    }
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
