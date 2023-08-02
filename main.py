import pandas as pd
from data_quality_checks import raw_to_OMFI_checks




checks = raw_to_OMFI_checks(feed_name="carefirst")
df = checks.medical_claims("C:/Users/Admin/Downloads/test_med.csv")
#print(df)


