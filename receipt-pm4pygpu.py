import pm4pygpu.dfg as dfg
import cudf
import timeit

df = cudf.read_parquet("receipt.parquet")
start = timeit.default_timer()
dfg = dfg.get_frequency_dfg(df)
end = timeit.default_timer()
print(dfg)
print(f"It took PM4PyGPU {end - start} seconds to discover the DFG")