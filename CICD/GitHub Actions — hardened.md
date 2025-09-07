```yaml
- name: Secret scan
  uses: trufflesecurity/trufflehog@v3
- name: SAST
  uses: github/codeql-action/init@v3
  with: {languages: python}
- name: Deploy Bundles (dev)
  run: |
    pip install databricks-sdk databricks-cli dbx
    databricks bundles deploy -t dev
```
