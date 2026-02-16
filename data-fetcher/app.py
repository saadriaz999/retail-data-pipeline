from flask import Flask, jsonify
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# load once (fast)
sales_df = pd.read_csv("data/fact_sales.csv")

@app.route("/sales/today", methods=["GET"])
def todays_sales():
    today = int(datetime.today().strftime("%Y%m%d"))
    result = sales_df[sales_df["date_id"] == today]
    return jsonify(result.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
