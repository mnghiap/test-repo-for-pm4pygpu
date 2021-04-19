import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import timeit
import sys

df = pd.read_parquet("bpic2019.parquet")
columns = df.columns
columns = [x.replace("AAA", ":") for x in columns]
df.columns = columns
log_name = sys.argv[1].replace(".parquet", ".csv")
df.to_csv(log_name)
log_csv = pd.read_csv(log_name)

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name'}
event_log = log_converter.apply(log_csv, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

start = timeit.default_timer()
dfg = dfg_discovery.apply(log_csv)
end = timeit.default_timer()
print(dfg)
print(f"It took PM4Py {end-start} seconds to discover the DFG")
