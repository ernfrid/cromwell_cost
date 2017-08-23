# cromwell_cost
Estimate cost of cromwell workflow on Google. Doesn't include network egress or
sustained usage discounts. Not all resource types included.

Resource usage is calculated by querying the genomics api using operations ids present in the cromwell metadata. The idea is based on comments made here: https://gatkforums.broadinstitute.org/firecloud/discussion/9130/cromwell-polling-interval-is-sometimes-too-long


## Requirements
python 2.7
pip install --upgrade google-api-python-client

## Usage

Given a pricelist json from Google and a metadata file from your workflow.

```python calculate.py pricelist.json metadata.json```

You may need to authorize application default credentials before running with: `gcloud auth application-default login`
