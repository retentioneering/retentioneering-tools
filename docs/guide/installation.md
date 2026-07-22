# Installation

## Requirements

- Python 3.10 or later
- Jupyter, VS Code, or Google Colab

## Install the package

```bash
pip install retentioneering
```

## Environment setup

### VS Code

Works out of the box — no additional setup required.

### JupyterLab and JupyterLab Desktop

Retentioneering widgets are built on [anywidget](https://anywidget.dev). For widgets to render correctly in JupyterLab, `anywidget` must be installed in the same Python environment that runs JupyterLab itself — not just in the kernel.

If you are using **JupyterLab Desktop**, open the built-in Extension Manager and make sure `anywidget` is listed there. If it is not, install it into the JupyterLab Desktop environment:

```bash
pip install anywidget
```

Then restart JupyterLab.

### Google Colab

Install the package at the top of your notebook:

```bash
!pip install retentioneering
```

## Verify the installation

```python
import retentioneering
print(retentioneering.__version__)
```
