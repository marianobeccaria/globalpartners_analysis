import pandas as pd

# First download some parquet files and sve it to /tmp
# e.g: 
#   aws s3 cp \
#         s3://mbeccaria-dea-globalpartners/gold/customer_rfm_segments/part-00000-7eebfb06-f6c3-40af-a66e-1f1ab6eb9e33-c000.snappy.parquet \
#       /tmp/rfm_sample.parquet 2>/dev/null || \
#   aws s3 cp \
#       $(aws s3 ls s3://mbeccaria-dea-globalpartners/gold/customer_rfm_segments/ | awk '{print "s3://mbeccaria-dea-globalpartners/gold/customer_rfm_segments/"$4}' | head -1) \
#       /tmp/rfm_sample.parquet


# Flagged Issue 1 - Ensure the tables are NOT empty
df = pd.read_parquet("/tmp/rfm_sample.parquet")

print(f"Total rows        : {len(df):,}")
print(f"\nSegment distribution:")
print(df["rfm_segment"].value_counts())
print(f"\nRecency stats (days):")
print(df["recency_days"].describe())