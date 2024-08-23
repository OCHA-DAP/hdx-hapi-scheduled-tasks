# hdx-hapi-scheduled-tasks

This repo checks the HDX HAPI resources endpoint for a list of resources used to populate HDX HAPI and then writes the `in_hapi` flag to those resources in HDX.

## Contributing

```shell
python -m venv venv
source venv/Scripts/activate
```

And then install the `requirements.txt`

```shell
pip install -r requirements.txt
```

The following environment variables need to be defined:
```
HAPI_BASE_URL
HDX_BASE_URL
HDX_API_KEY
```