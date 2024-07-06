# **README.md**

**FOLLOWING STEP SETUP**

1). **Install:**

```
pip install reformat-app
```

2). **Create Folder:**

```
1. Create folder name: reformat_app
2. Create sub-folder name: config
3. Create sub-folder name: template
4. Copy file: config.yaml and logging_config to sub-folder: config
5. Copy file: Application Data Requirements.xlsx to sub-folder: template
```

3). **Generate code python:**

```
from reformat_app import main

try:
    main()
except Exception as err:
    print(err)
```

4). **Run code python:**

```
python main.py
```
