import pm4pygpu.dfg as dfg
import pm4pygpu.format as format
import cudf
import timeit

df = cudf.read_parquet("receipt.parquet")
df = format.apply(df)
start = timeit.default_timer()
dfg = dfg.get_frequency_dfg(df)
end = timeit.default_timer()
print(dfg)
print(f"It took PM4PyGPU {end - start} seconds to discover the DFG")