# test config for tfx parquet example reader for reading from delta lake
output_config = example_gen_pb2.Output(
    split_config=example_gen_pb2.SplitConfig(splits=[
        example_gen_pb2.SplitConfig.Split(name='train', hash_buckets=3)
        ,example_gen_pb2.SplitConfig.Split(name='eval', hash_buckets=1)
    ]))
    
input_config = example_gen_pb2.Input(splits=[
    example_gen_pb2.Input.Split(name='parquet', pattern='*.parquet')])

example_gen = FileBasedExampleGen(
    custom_executor_spec=executor_spec.ExecutorClassSpec(
    parquet_executor.Executor),
    input_base="~/mlops/dataset/delta-lake/silver",
    input_config=input_config,
    output_config=output_config).with_id('ParquetExampleGen')