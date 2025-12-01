from datasets import load_dataset
data = load_dataset("range3/wiki40b-ja")
print(data)
# DatasetDict({
#     train: Dataset({
#         features: ['wikidata_id', 'text', 'version_id'],
#         num_rows: 745392
#     })
#     validation: Dataset({
#         features: ['wikidata_id', 'text', 'version_id'],
#         num_rows: 41576
#     })
#     test: Dataset({
#         features: ['wikidata_id', 'text', 'version_id'],
#         num_rows: 41268
#     })
# })
