# cromwell_cost
Estimate cost of cromwell workflow on Google. Doesn't include network egress or
sustained usage discounts. Not all resource types included.

## Requirements
python 2.7
pip install --upgrade google-api-python-client

## Usage

Given a pricelist json from Google and a metadata file from your workflow.

```python calculate.py pricelist.json metadata.json```
