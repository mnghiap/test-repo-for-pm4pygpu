import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import timeit
import sys

df = pd.read_parquet(sys.argv[1])
columns = df.columns
columns = [x.replace("AAA", ":") for x in columns]
df.columns = columns

parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name'}
event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

start = timeit.default_timer()
dfg = dfg_discovery.apply(df)
end = timeit.default_timer()
print(dfg)
print(f"It took PM4Py {end-start} seconds to discover the DFG")
