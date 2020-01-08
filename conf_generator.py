import numpy as np
import pandas as pd
import random

# np.set_printoptions(precision=2, linewidth=100, suppress=True)
# pd.options.display.float_format = '{:,.2f}'.format
# pd.set_option('display.width',500)

## Number of configurations
n = 2000

## Randomly generate configurations
spark_reducer_maxSizeInFlight_range = range(2, 128, 2)  # with m
spark_reducer_maxSizeInFlight_sample = random.choices(spark_reducer_maxSizeInFlight_range, k=n)
spark_reducer_maxSizeInFlight_sample = [str(c) + 'm'  for c in spark_reducer_maxSizeInFlight_sample]

spark_shuffle_file_buffer_range = range(2, 128, 2)  # with k
spark_shuffle_file_buffer_sample = random.choices(spark_shuffle_file_buffer_range, k=n)
spark_shuffle_file_buffer_sample = [str(c) + 'k'  for c in spark_shuffle_file_buffer_sample]

spark_shuffle_sort_bypassMergeThreshold_range = range(100, 1000, 100)
spark_shuffle_sort_bypassMergeThreshold_sample = random.choices(spark_shuffle_sort_bypassMergeThreshold_range, k=n)

spark_speculation_interval_range = range(100, 1000, 100)  # with ms
spark_speculation_interval_sample = random.choices(spark_speculation_interval_range, k=n)
spark_speculation_interval_sample = [str(c) + 'ms'  for c in spark_speculation_interval_sample]

spark_speculation_multiplier_range = np.arange(1, 5, 0.5)
spark_speculation_multiplier_sample = random.choices(spark_speculation_multiplier_range, k=n)

spark_speculation_quantile_range = np.arange(0.25, 1, 0.25)
spark_speculation_quantile_sample = random.choices(spark_speculation_quantile_range, k=n)

spark_broadcast_blockSize_range = np.arange(2, 128, 2)  # with m
spark_broadcast_blockSize_sample = random.choices(spark_broadcast_blockSize_range, k=n)
spark_broadcast_blockSize_sample = [str(c) + 'm'  for c in spark_broadcast_blockSize_sample]

spark_io_compression_snappy_blockSize_range = np.arange(2, 128, 2)  # with k, snappy by default, so no lz4
spark_io_compression_snappy_blockSize_sample = random.choices(spark_io_compression_snappy_blockSize_range, k=n)
spark_io_compression_snappy_blockSize_sample = [str(c) + 'k'  for c in spark_io_compression_snappy_blockSize_sample]

spark_kryoserializer_buffer_max_range = np.arange(8, 128, 8)  # with m
spark_kryoserializer_buffer_max_sample = random.choices(spark_kryoserializer_buffer_max_range, k=n)
spark_kryoserializer_buffer_max_sample = [str(c) + 'm'  for c in spark_kryoserializer_buffer_max_sample]

spark_kryoserializer_buffer_range = np.arange(2, 128, 2)  # with k
spark_kryoserializer_buffer_sample = random.choices(spark_kryoserializer_buffer_range, k=n)
spark_kryoserializer_buffer_sample = [str(c) + 'k'  for c in spark_kryoserializer_buffer_sample]

spark_driver_memory_range = np.arange(1, 12, 1)  # with g
spark_driver_memory_sample = random.choices(spark_driver_memory_range, k=n)
spark_driver_memory_sample = [str(c) + 'g'  for c in spark_driver_memory_sample]

spark_executor_memory_range = np.arange(1, 12, 1)  # with g
spark_executor_memory_sample = random.choices(spark_executor_memory_range, k=n)
spark_executor_memory_sample = [str(c) + 'g'  for c in spark_executor_memory_sample]

spark_network_timeout_range = np.arange(20, 500, 20) # with s
spark_network_timeout_sample = random.choices(spark_network_timeout_range, k=n)
spark_network_timeout_sample = [str(c) + 's'  for c in spark_network_timeout_sample]

spark_locality_wait_range = np.arange(1, 10, 1) # with s
spark_locality_wait_sample = random.choices(spark_locality_wait_range, k=n)
spark_locality_wait_sample = [str(c) + 's'  for c in spark_locality_wait_sample]

spark_task_maxFailures_range = np.arange(1, 8, 1)
spark_task_maxFailures_sample = random.choices(spark_task_maxFailures_range, k=n)

spark_shuffle_compress_range = ['false', 'true']  # true or false
spark_shuffle_compress_sample = random.choices(spark_shuffle_compress_range, k=n)

spark_memory_fraction_range = np.arange(0.5, 1, 0.1)
spark_memory_fraction_sample = random.choices(spark_memory_fraction_range, k=n)

spark_shuffle_spill_compress_range = ['false', 'true']  # true or false
spark_shuffle_spill_compress_sample = random.choices(spark_shuffle_spill_compress_range, k=n)

spark_broadcast_compress_range = ['false', 'true'] # true or false
spark_broadcast_compress_sample = random.choices(spark_broadcast_compress_range, k=n)

spark_memory_storageFraction_range = np.arange(0.5, 1, 0.1)
spark_memory_storageFraction_sample = random.choices(spark_memory_storageFraction_range, k=n)

## Generate configuration table
zippedList =  list(zip(spark_reducer_maxSizeInFlight_sample,
                       spark_shuffle_file_buffer_sample,
                      spark_shuffle_sort_bypassMergeThreshold_sample,
                      spark_speculation_interval_sample,
                      spark_speculation_multiplier_sample,
                      spark_speculation_quantile_sample,
                      spark_broadcast_blockSize_sample,
                      spark_io_compression_snappy_blockSize_sample,
                      spark_kryoserializer_buffer_max_sample,
                      spark_kryoserializer_buffer_sample,
                      spark_driver_memory_sample,
                      spark_executor_memory_sample,
                      spark_network_timeout_sample,
                      spark_locality_wait_sample,
                      spark_task_maxFailures_sample,
                      spark_shuffle_compress_sample,
                      spark_memory_fraction_sample,
                      spark_shuffle_spill_compress_sample,
                      spark_broadcast_compress_sample,
                      spark_memory_storageFraction_sample))
df = pd.DataFrame(zippedList, columns = ['spark.reducer.maxSizeInFlight',
                                         'spark.shuffle.file.buffer',
                                        'spark.shuffle.sort.bypassMergeThreshold',
                                        'spark.speculation.interval',
                                        'spark.speculation.multiplier',
                                        'spark.speculation.quantile',
                                        'spark.broadcast.blockSize',
                                        'spark.io.compression.snappy.blockSize',
                                        'spark.kryoserializer.buffer.max',
                                        'spark.kryoserializer.buffer',
                                        'spark.driver.memory',
                                        'spark.executor.memory',
                                        'spark.network.timeout',
                                        'spark.locality.wait',
                                        'spark.task.maxFailures',
                                        'spark.shuffle.compress',
                                        'spark.memory.fraction',
                                        'spark.shuffle.spill.compress',
                                        'spark.broadcast.compress',
                                        'spark.memory.storageFraction'])
df.to_csv("conf_chaoqin.csv")
