from pyspark.sql import SparkSession


if __name__ == "__main__": 

    spark = SparkSession.builder\
                .master("local[*]")\
                .appName("join-strategies")\
                .getOrCreate()

    df_clients = spark.createDataFrame([
        (0, "client1"),
        (1, "client2"),
        (2, "client3")])\
    .toDF("client_id", "name")

    df_orders = spark.createDataFrame([
        (0, "order1", 100),
        (1, "order2", 200),
        (2, "order3", 150)])\
    .toDF("client_id", "order_id", "order_amount")


    df_clients.printSchema()
    df_orders.printSchema()

    # 
    """
    sort_merge_join = df_clients.join(df_orders, on='client_id', how='inner')
    sort_merge_join.explain(mode="formatted")
    sort_merge_join.show()
    """

    ## Broadcast
    """
    broadcast_df = df_clients.hint('broadcast').join(df_orders, on='client_id', how='inner')
    broadcast_df.explain(mode="formatted")
    broadcast_df.show()
    """

    ## ShuffledHashJoin
    """
    shuffle_hashed_df = df_clients.hint('SHUFFLE_HASH').join(df_orders, on='client_id', how='inner')
    shuffle_hashed_df.explain(mode="formatted")
    shuffle_hashed_df.show()
    """

     ## CartesianProduct 
    cartesian_product_join_df  = df_clients.hint('SHUFFLE_REPLICATE_NL').join(df_orders, on='client_id', how='inner')
    cartesian_product_join_df.explain(mode="formatted")
    cartesian_product_join_df.show()


    import time
    time.sleep(300)

    spark.stop()
