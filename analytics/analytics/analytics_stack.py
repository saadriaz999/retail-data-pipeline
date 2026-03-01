from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_glue as glue,
    aws_s3 as s3,
    aws_athena as athena,
)
from constructs import Construct


def _parquet_serde():
    return glue.CfnTable.SerdeInfoProperty(
        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    )


def _parquet_storage(location: str, columns: list, partition_keys=None):
    return glue.CfnTable.StorageDescriptorProperty(
        columns=columns,
        location=location,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_info=_parquet_serde(),
    )


class AnalyticsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3: Athena query results
        athena_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name="retail-athena-results",
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Glue database
        db_name = "retail_db"
        glue_database = glue.CfnDatabase(
            self,
            "RetailDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=db_name,
                description="Retail star schema",
            ),
        )

        # fact_sales
        fact_sales_table = glue.CfnTable(
            self,
            "FactSalesTable",
            catalog_id=self.account,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="fact_sales",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_parquet_storage(
                    "s3://retail-processed-dataset/fact_sales/",
                    [
                        glue.CfnTable.ColumnProperty(name="customer_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="device_type", type="string"),
                        glue.CfnTable.ColumnProperty(name="discount", type="double"),
                        glue.CfnTable.ColumnProperty(name="order_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="payment_method", type="string"),
                        glue.CfnTable.ColumnProperty(name="product_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="quantity", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="sale_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="store_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="total_amount", type="double"),
                        glue.CfnTable.ColumnProperty(name="unit_price", type="double"),
                        glue.CfnTable.ColumnProperty(name="processed_timestamp", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="date", type="date"),
                    ],
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="int"),
                    glue.CfnTable.ColumnProperty(name="month", type="int"),
                ],
            ),
        )
        fact_sales_table.add_dependency(glue_database)

        # dim_customer
        dim_customer_table = glue.CfnTable(
            self,
            "DimCustomerTable",
            catalog_id=self.account,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="dim_customer",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_parquet_storage(
                    "s3://retail-processed-dataset/dimensions/dim_customer/",
                    [
                        glue.CfnTable.ColumnProperty(name="customer_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="country", type="string"),
                        glue.CfnTable.ColumnProperty(name="language", type="string"),
                    ],
                ),
            ),
        )
        dim_customer_table.add_dependency(glue_database)

        # dim_product
        dim_product_table = glue.CfnTable(
            self,
            "DimProductTable",
            catalog_id=self.account,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="dim_product",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_parquet_storage(
                    "s3://retail-processed-dataset/dimensions/dim_product/",
                    [
                        glue.CfnTable.ColumnProperty(name="product_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="product_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="category", type="string"),
                        glue.CfnTable.ColumnProperty(name="subcategory", type="string"),
                        glue.CfnTable.ColumnProperty(name="brand", type="string"),
                        glue.CfnTable.ColumnProperty(name="unit_cost", type="double"),
                    ],
                ),
            ),
        )
        dim_product_table.add_dependency(glue_database)

        # dim_store
        dim_store_table = glue.CfnTable(
            self,
            "DimStoreTable",
            catalog_id=self.account,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="dim_store",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_parquet_storage(
                    "s3://retail-processed-dataset/dimensions/dim_store/",
                    [
                        glue.CfnTable.ColumnProperty(name="store_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="store_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="city", type="string"),
                        glue.CfnTable.ColumnProperty(name="state", type="string"),
                        glue.CfnTable.ColumnProperty(name="country", type="string"),
                    ],
                ),
            ),
        )
        dim_store_table.add_dependency(glue_database)

        # dim_date
        dim_date_table = glue.CfnTable(
            self,
            "DimDateTable",
            catalog_id=self.account,
            database_name=db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="dim_date",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_parquet_storage(
                    "s3://retail-processed-dataset/dimensions/dim_date/",
                    [
                        glue.CfnTable.ColumnProperty(name="date_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="full_date", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="day", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="month", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="year", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="quarter", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="day_of_week", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="is_weekend", type="boolean"),
                    ],
                ),
            ),
        )
        dim_date_table.add_dependency(glue_database)

        # Athena workgroup
        athena.CfnWorkGroup(
            self,
            "RetailAthenaWorkgroup",
            name="retail-analytics",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_bucket.bucket_name}/queries/",
                ),
            ),
            state="ENABLED",
        )