# Installation

```
conda create -n jerry-env python=3.9
conda activate jerry-env
pip install -r requirements.txt
cp jars/*.jar $(python -c "import pyspark; print(pyspark.__path__[0])")/jars
```