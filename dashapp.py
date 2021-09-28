import dash
import dash_table
import dash_html_components as html
import dask.dataframe as dd
import pandas as pd
from pandas import DataFrame
import numpy as np


def processData() -> DataFrame:
    cohort = dd.from_pandas(pd.read_parquet(r"data/cohort_data"), npartitions=3)
    clickstream = dd.from_pandas(
        pd.read_parquet(r"data/clickstream_data"), npartitions=5
    )
    account = dd.from_pandas(pd.read_parquet(r"data/accounting_data"), npartitions=5)
    data = (
        cohort[["user_id", "cohort_id", "cohort_name"]]
        .merge(
            clickstream[
                [
                    "user_id",
                    "facility_id",
                    "market_id",
                    "market_name",
                    "event_start_timestamp",
                    "event_end_timestamp",
                ]
            ],
            on="user_id",
            how="inner",
        )
        .merge(
            account[
                [
                    "facility_id",
                    "revenue_amount",
                    "transaction_timestamp",
                    "processing_timestamp",
                ]
            ],
            on="facility_id",
            how="inner",
        )
        .query(
            "transaction_timestamp >= event_start_timestamp and transaction_timestamp <= event_end_timestamp "
        )
    )
    data["week"] = data["processing_timestamp"].apply(
        lambda x: x.strftime("%V"), meta=("processing_timestamp", "object")
    )
    df = data.compute()
    output = (
        df.groupby(["market_id", "cohort_id", "week"])
        .agg(lambda x: x.sum() if x.name in ["revenue_amount"] else x.head(1))
        .reset_index()
    )
    output["max"] = output.groupby(["market_id", "week"])["revenue_amount"].transform(
        "max"
    )
    output["flag"] = np.where((output["revenue_amount"] == output["max"]), True, np.nan)
    return output[output["flag"] == True][
        ["market_name", "cohort_name", "revenue_amount", "week"]
    ]


df = (
    processData()
    .round(decimals=2)
    .sort_values(["market_name", "week"], ascending=[True, True])
)

app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H1(
            "Top Performing Cohort",
        ),
        dash_table.DataTable(
            columns=[
                {"name": "Market name", "id": "market_name", "type": "text"},
                {"name": "Cohort Name", "id": "cohort_name", "type": "text"},
                {"name": "Revenue", "id": "revenue_amount", "type": "numeric"},
                {"name": "Week", "id": "week", "type": "numeric"},
            ],
            data=df.to_dict("records"),
            filter_action="native",
            style_table={
                "height": 400,
            },
            style_data={
                "width": "150px",
                "minWidth": "150px",
                "maxWidth": "150px",
                "overflow": "hidden",
                "textOverflow": "ellipsis",
            },
        ),
    ]
)


if __name__ == "__main__":
    app.run_server(debug=True,host='0.0.0.0', port=8050)



