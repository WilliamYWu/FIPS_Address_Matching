---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.4
kernelspec:
  display_name: Python 3
  name: python3
---

+++ {"id": "v_-BsdBDEC7J"}

# **Tutorial: All About XGBoost**

Welcome! This tutorial will introduce the core code and features for the machine learning model: XGBoost. In particular, we will focus on introducing the XBGClassifier model derived from the xgboost library.

This tutorial will cover:
* Utilizing the Kaggle API token
* Employing shell commands to create a flexible directory structure
* Standard file organization and setup convention
* Basic XGBoost Model
* Understanding important parameters inside XGBoost
* Hyperparameter Optimization
* Evaluating our Optimized XGBoost Model

Whats needed for setup:
* Grab your Kaggle API Token which you'll find on your Kaggle account page

Reference
* [Slides](https://www.overleaf.com/read/nvzjfjqqbxxs)

+++ {"id": "xxEASe5PaIUj"}

# Section 1

1.   Setup


+++ {"id": "oWFiKWHokLQ4"}

## Section 1.1: Setup
* Setting up basic directory structure
* Creating and installing requirements
* Importing packages
* Setting up the Kaggle Library for use

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: 4ytSZbnVfyhr
outputId: cd88f84b-1046-409d-d556-b1203d832db8
---
from google.colab import files, drive
import os
# Mounting will give your collab notebook access to the directory provided which is this case is your drive
drive.flush_and_unmount()
drive.mount("/content/drive")
# NOTE: Need the following two lines to reconfirm directory, since if you want to rerun this cell you will run into errors with mkdir
os.chdir("/content")
os.chdir("/")

# mkdir will create the directories provided. 
# -p is an option which enables the command to create parent directories as necessary. If the directories exist, no error is specified. 
# If -p doesn't work for you due to your operating system feel free to remove it. The command works without it
!mkdir -p "/content/drive/My Drive/Colab Notebooks/Kagglethon_XGBoost/"

%cd "/content/drive/My Drive/Colab Notebooks/Kagglethon_XGBoost/"
# Touch creates the file at the procided directory
!touch requirements.txt

# Another option other than using !pip install every package. 
# Instead create a text file called requirements and install that instead.
# Google Colab comes with a pre-installed, but older version of Pandas-profiling (now called ydata-profiling) where the join_axes function is deprecated in concat()
packages = ["latexify-py==0.2.0", 
            "pandas", 
            "numpy", 
            "ydata-profiling",
            "regex", 
            "hyperopt", 
            "kaggle", 
            "matplotlib",
            "seaborn", 
            "xgboost", 
            "scikit-learn"]

with open("requirements.txt", "w") as file:
  for package in packages:
    file.write(f"{package}\n")
file.close()
```

```{code-cell}
:id: yn7SMMG4DckR

!pip install -q -r "requirements.txt"
```

```{code-cell}
:id: VWtbSD0gDj9g

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Pandas-Profiling had an update
from ydata_profiling import ProfileReport

import xgboost as xgb
from xgboost import XGBClassifier, XGBRegressor
from xgboost import plot_importance

from sklearn import model_selection
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.utils import class_weight


from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

from sklearn.metrics import accuracy_score, balanced_accuracy_score, roc_auc_score, make_scorer
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import plot_confusion_matrix

import re
import latexify
import warnings
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: UGXuWrchb-9-
outputId: 38a203f9-dfad-455b-d0c8-7d8a7d41fbdf
---
MAIN_DIR = os.getcwd()
print(MAIN_DIR)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 475
id: dBWgHUEGFv-6
outputId: 59152e25-c9d3-4761-ab95-93d3b24c9075
---
# NOTE: Need to download the kaggle token json before you can work in this cell

# If we an extra file already in the directory this function will remove those files
# If you end up running this cell a lot you'll end up with a lot of kaggle.json files
# Works for any possible file you upload through files.upload()
def upload_replace(api_key, dir):
  name = next(iter(api_key.keys())).rsplit(".", maxsplit=1)[0]
  for file in os.listdir(dir):
    if bool(re.search(f"{name}\s\(\d+\).[a-z]+",file)):
      os.remove(file)

# Allows you to upload a file
api_key = files.upload()
upload_replace(api_key, MAIN_DIR)

# Needed for error supression potentially
# Kaggle is a very sensitive api that breaks if your directories aren't setup correctly
!mkdir -p ~/.kaggle
!chmod 600 ~/.kaggle/kaggle.json
!cp "$MAIN_DIR/kaggle.json" ~/.kaggle/kaggle.json

!kaggle datasets list
```

```{code-cell}
:id: cb84KS9zza4c

ZIP_DATA_DIR = MAIN_DIR + "/zip_data"
RAW_DATA_DIR = MAIN_DIR + "/raw_data"
FIGURES_DIR = MAIN_DIR + "/figures"
```

+++ {"id": "kwt54VDNTeV2"}

# Section 2

1. Example 1 -  Basic XGBoost Model
2. Food For Thought
3. Intro to XGBoost
4. Intuitive Explanation of Gradient Boosting (Slides)
4. Intuitive Explanation of XGBoost (Slides)

+++ {"id": "g9H5RKxfkkIH"}

 ## Section 2.1: Example 1 - Basic XG Boost Model
 * Basic code for XGBoost

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: Mux0eFWycobe
outputId: 3ac1a499-b327-4492-854a-0a13e5075447
---
%cd "$MAIN_DIR"
# Specific directory made for every zip file
!mkdir -p "$ZIP_DATA_DIR/pima_diabetes"

# Every dataset has an api-key reference that you'll find in the three dot menu next to download
!kaggle datasets download -d "uciml/pima-indians-diabetes-database"
!mv pima-indians-diabetes-database.zip "$ZIP_DATA_DIR/pima_diabetes"
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: qxItLGA5eCxQ
outputId: 33aadcdc-4ff2-48f2-e328-30cda49c158b
---
# Specific directory made for you raw unzipped data
!mkdir -p "$RAW_DATA_DIR/pima_diabetes"
!unzip -o "$ZIP_DATA_DIR/pima_diabetes/pima-indians-diabetes-database.zip" -d "$RAW_DATA_DIR/pima_diabetes"
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 206
id: dfEd--Ceex8a
outputId: de8d3d62-02c6-4c35-ee39-06f85ff7aa05
---
df = pd.read_csv(f"{RAW_DATA_DIR}/pima_diabetes/diabetes.csv")
df.head()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 949
  referenced_widgets: [343df6aa78154b60af3a4a1dac3a5609, 53c9498fded44445bb2f9919c9e44832,
    f55a694a04e04ead95ca3c5172778cc0, 4ce8673f537c452b81eb1aa049057079, f59d212ff3db45588006482d03b90aa3,
    d09fc39e066340af81e8950daff4f368, 39a657f3768a44309f21a12b8f2dd51a, 5464f53445ae4f488caf9e4dd6532408,
    1c8e051127af450f8a5e0b9271bfcd22, 02e3d0ad20e4492bbeca572fb764ca7b, e56cb0006a2c42318ae48a815f952869,
    b892a3be8a7c483bafe1430f25fa6b37, 7b3a81435a2d4eb7a373708183105922, d4dc2099535a4f64b13c10ba5d3e7602,
    7b5f7980a3164ce08002f5531fcc9ad9, 9578792586074423bedc7563cd75cf45, 2221a944f38e4d768ef1297f34b02c46,
    529c3241955845d69e15975ddd6ef8ba, 4a1bd83b467a4a8b8587d86603f49905, b7365484cd8e4296ab5753bafe9a88a7,
    704c816fc1fc446c9056681fd9425421, 546afb7587c248aaadd37bfdc7701411, 9e340504563a4966a354abd38f97058f,
    f3cd2d1fd3d344fb901b1ea789fd850c, 34ca7b04e50a465da721e91cc5e0df20, e44415590f95407e9fea3ed035e532b0,
    bb3534ee960e4c86adca98a702f312b3, a6f91273c82a43f78bba85e00841bf69, 3d90864c423b4f4b98f1c9fc10f6ab99,
    6ced59485a0743488d33810a04684fc9, d3de9784dd314e6eb38a3f31650b6614, 75e5b584a9da43d985438dc20cbe72a2,
    3ad51f91970b41beb0dd2b896a88aded, b6a5f63fda44442eb2f630c2bdc40011, c97381f322c649e2a137ccf5d0f7481b,
    fb8bd204e7854712a63a80195dc02640, 71b57b37447944ecb82d99b950a77c70, 94c0935ede8744078a147898f6454da4,
    0b45f3385d4a46309bea2279aa29b22b, 845db6be1f9a4de689886cedf127fe0b, 1580ac92699148e5a988cad70740a5d4,
    cfadb65bacde4a948327c3b5f563bc60, f044d2019e3d4ddd8f435e496c16f02d, d0ab882fb64541a9b17f3b1acc198bfc]
id: v4C1SepLTb5o
outputId: 4daa2984-c37c-4563-dca3-59eb1b5fc698
---
!mkdir -p "$FIGURES_DIR/pima_diabetes"

profile = ProfileReport(df)
profile.to_file(output_file = f"{FIGURES_DIR}/pima_diabetes/diabetes.html")
profile.to_notebook_iframe()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 206
id: wBRWh43tgh2b
outputId: 976871f7-2b83-4aca-abd8-ac15f0f2d32c
---
# This is a technique called masking
# Personal opinion -> This is the cleanest way to filter your DataFrame
# T stands for transpose, since a DataFrame is a matrix
X = df.T[df.columns != "Outcome"].T
Y = df.T[df.columns == "Outcome"].T
X.head()
```

```{code-cell}
:id: XZRBlypEhbst

seed = 10 # Used to initialize the random number generator so that you get the same random numbers every time
test_size = 0.2 # Percentage used for the train/test split
x_train, x_test, y_train, y_test = train_test_split(X,Y,test_size=test_size, random_state=seed) 
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: MoPhhhDCiaN3
outputId: 21167e25-243a-4f6f-f091-4583bb0e87ea
---
# Here is our model
model = XGBClassifier()
# Fit Model on Training Data
# y_train needs to be an array rather than a vector -> you can also this of this as a list rather than a DataFrame
# Ravel will return a 1D flattened array
model.fit(x_train, y_train.values.ravel())
# Make Predicitons for Test Data
predictions = model.predict(x_test)
# Evaluate Predictions
accuracy = accuracy_score(y_test, predictions)
print(f"Accuracy: {(accuracy * 100.0):.2f}")
```

+++ {"id": "QsX4KDHoj02Q"}

## Section 2.2: Food For Thought
* What did we just do? 
* Wasn't this pretty easy?
* Auto ML

+++ {"id": "rQ4V6BRgaiSL"}

## Section 2.3: Intro to XGBoost

XGBoost is one of the most powerful techniques for building predictive models. It has been used to win many Kaggle competitions and now its one of the premier models used in research and industry. However, XGBoost is an algorithm thats built on top of other algorithms. 

**1. Introduction**
* XGBoost or eXtreme Gradient Boosting was implemented from gradient boosted decision trees designed for speed and performance.
* Quite fast compared to most other models.
* Strong performance on tabular classification and regression predictive modeling problems. 
* Sparse Aware Implementation and automatically deals with missing values.
* You'll see the above points shown in a later demonstration in Section 3.4.
* Built in regularization to prevent overfitting.
* Allows for tree pruning.
* Can use your own customized objective function.
* When we go over the math we will look at this.
* There are many more unique features but those go beyond the scope of what is explained in this class and whats provided here is just a large generalization of the capabilities that XGBoost has.

**2. Gradient Boosting**
* Based off of AdaBoost which is based off of decision trees.
* Optimizes a loss function.
* Uses a weak learner to make predictions.
* A weak learner is one that learns at the very least slightly better than random chance. 
* Here the weak learner is a decision tree, specifically a regression tree.
* Continues adding weak learners to minimize the loss function. 
* Essentially, you use many trees to predict the residuals, so that the model is slow to overfit. 

+++ {"id": "elTulsm2HpTI"}

## Section 2.4: Intuitive Explanation of Gradient Boosting

Please refer to slides

+++ {"id": "pgzZRi7FHpV5"}

## Section 2.5: Intuitive Explanation of XGBoost

Please refer to slides

+++ {"id": "H9QlYRaKktd-"}

# Section 3

1. Example 2 - XGBoost Parameters
2. Objective Functions with Mathematical Interpretation
3. Other Parameters: Learning Rate, Tree Depth, Number of Trees, Gamma and Lambda

+++ {"id": "7yK60VFKk--d"}

## Section 3.1: Example 2 - XGBoost Parameters

* Modeling with non-numerical data
* Understanding the parameters behind XG Boost

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: im_FAOWxinVD
outputId: b02c5632-7cdc-4516-db55-9661f925cae7
---
!kaggle datasets download -d uciml/iris
!mkdir -p "$ZIP_DATA_DIR/iris"
!mkdir -p "$RAW_DATA_DIR/iris"

!mv iris.zip "$ZIP_DATA_DIR/iris"
!unzip -o "$ZIP_DATA_DIR/iris/iris.zip" -d "$RAW_DATA_DIR/iris"
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 206
id: q7M8hqWcm_iM
outputId: 540cd6c9-0dd4-488c-88eb-c4864c0564d3
---
df = pd.read_csv(f"{RAW_DATA_DIR}/iris/Iris.csv")
df.head()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 206
id: _WClySk85jtq
outputId: d34dd9e0-99a8-4be4-b417-0dffedb3ef00
---
# Feel free to try it the way we did it in Example 1 you'll find that by transposing we end up changing the datatypes to objects which causes problems for us later on in the model.
# Another way to do this could be using column index instead since you know that Species is the last column. df.loc[:,"Species"] or df.iloc[:,0:4]
X = df[df.columns[df.columns != "Species"]]
Y = df[df.columns[df.columns == "Species"]]
X.head()
```

```{code-cell}
:id: dfpTIOQt60BI

# LabelEncoder will encode the textual data into an integer
# In this case Y_encoded will store the data as 0 = Iris-setose, 1 = Iris-versicolor, 2 = Iris-virginica
label_encoder = LabelEncoder()
label_encoder = label_encoder.fit(Y.values.ravel())
Y_encoded = label_encoder.transform(Y.values.ravel())
```

```{code-cell}
:id: ARWr3qR86pfP

seed = 10
test_size = 0.2
x_train, x_test, y_train, y_test = train_test_split(X,Y_encoded,test_size=test_size, random_state=seed)
```

```{code-cell}
:id: PFg2BlLJACMw

model = XGBClassifier(base_score=0.5,               # Initial prediction score of all instances, global bias
                      colsample_bylevel=1,          # Random fraction of features to train each node to train each tree
                      colsample_bytree=1,           # Random fraction of features to train each tree
                      gamma=0,                      # Used to prune your trees
                      learning_rate=0.1,            # Shrinkage to prevent overfitting
                      max_delta_step=0,             
                      max_depth=3,                  # Depth of Tree. The larger the more complex and model is more likely to overfit.
                      min_child_weight=1,           
                      missing=None,                 
                      n_estimators=100,             # Number of Trees
                      nthread=-1,                   # Number of Threads used for multithreading -> Parallel Processing -> By default it does the max
                      objective="binary:logistic",  # Your objective function -> Loss Function
                      reg_alpha=0,                  # L1 Regularization or Lasso Regression
                      reg_lambda=1,                 # L2 Regularization or Ridge Regression
                      scale_pos_weight=1,           
                      seed=0,                       
                      silent=True,                  
                      subsample=1)                  # Random subsample ratio of the training sample to train each tree
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: JIvYg5kNABLY
outputId: 7d8a2648-8b71-4567-c7cf-dd3339f81ee0
---
model.fit(x_train, y_train.ravel())
predictions = model.predict(x_test)
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0)) 
```

+++ {"id": "HU8GUYy8XSLa"}

##Section 3.2: Example 3 - Objective Functions
The objective is also known as the loss function. Typically in machine learning the loss function is how we evaluate the training error. As a result, when we say that we aim to minimize the error, we are trying to minimize the loss function. 
* List of the objectives used in xgboost: [All Provided Objectives](
https://xgboost.readthedocs.io/en/latest/parameter.html#learning-task-parameters.)
* There will be some math involved in this section when going over the second order derivative of our objective function.

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: tl2O7cmslUaG
outputId: 8da15697-7c12-41c0-aae0-9c44c8f30a37
---
'''
 If you get a 403 - Forbidden Response
 You might need to scroll down on the page https://www.kaggle.com/competitions/house-prices-advanced-regression-techniques/data
 Look at the data descriptions section and Agree to the Data Usage Policy
'''
!kaggle competitions download -c house-prices-advanced-regression-techniques
!mkdir -p "$ZIP_DATA_DIR/house_price"
!mkdir -p "$RAW_DATA_DIR/house_price"


!mv house-prices-advanced-regression-techniques.zip "$ZIP_DATA_DIR/house_price"

# Specific directory made for you raw unzipped data
!unzip -o "$ZIP_DATA_DIR/house_price/house-prices-advanced-regression-techniques.zip" -d "$RAW_DATA_DIR/house_price"
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 299
id: AyG2x8GQEuhi
outputId: e085defd-489a-4f9e-a5d4-dd2a1b123540
---
train_df = pd.read_csv(f"{RAW_DATA_DIR}/house_price/train.csv")
train_df.fillna(0, inplace=True)
train_df.head()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: ujJLcVSSViJa
outputId: 06bcc815-ca8f-472e-db63-1bdd40e134b8
---
# Notice how we have object types
# XGBoost only allows for numerical or boolean values
# Even though its capable of handling missing values as long as every missing value has the same value, you still need to encode the categorical variables
print(train_df.dtypes)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 299
id: Lqyv4zLTWe85
outputId: 1a393ff6-32ed-4984-bb97-e5441b26becb
---
# Instead of One-Hot-Encoding which requires a lot more work to get working on this dataset, since it requires every variable to be encoded
# Pandas inherently has a method called get_dummies that will do the same thing for us with much less hassle 
train_df = pd.get_dummies(train_df)
train_df.head()
```

```{code-cell}
:id: Qu9uQCgUIwk5

# This is a regression question since we're trying to predict the actual value rather than a class like with the Iris Example
X = train_df[train_df.columns[train_df.columns != "SalePrice"]]
Y = train_df[train_df.columns[train_df.columns == "SalePrice"]]

seed = 10
test_size = 0.2
x_train, x_test, y_train, y_test = train_test_split(X,Y,test_size=test_size, random_state=seed)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 35
id: SGGO2c__EfDY
outputId: b2fc7d5d-5faa-47c1-ba83-c6b36a117920
---
# Since this is regression rather than classification we should use XGBRegressor rather than XGBClassifier
# Notice how I'm assigning the objective to be "reg:squarederror"
model = XGBRegressor(objective='reg:squarederror')
model.objective
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: DK3C8DEQXeea
outputId: 7b9a8d0f-a6db-4a6b-cbe6-b120c61884dc
---
model.fit(x_train, y_train)
predictions = model.predict(x_test)

# We can't use accuracy since we're working with Regression
# Accuracy measures what percent was given the correct classification.

# Calculating the R^2 or coefficient of Determination
r2 = r2_score(y_test, predictions)
print(f"r2_score: {r2 * 100.0:.2f}%")

# The RMSE is the standard deviation of the residuals. 
# It shows how far predictions fall from the measured true values using Euclidian distance
rmse = np.sqrt(mean_squared_error(y_test, predictions))
print(f"root_mean_squared_error:{rmse}")
```

```{code-cell}
:id: aKVPl6fkXZ46

'''
y_p: Predicted values of a matrix y
y_a: Actual values of a matrix y
'''
# Latex is a language used to program scientific documents
# It's how your math teacher makes his tests or how Dr.Kurmann makes his slides
@latexify.function
def mse(y_p, y_a):
    
    # Here is our generalized objective function
    # l(y_val, y_pred) = (y_a-y_p)**2
    objective = (y_a-y_p)**2

    # This stands for gradient or the first order derivative of our objective function
    grad = 2 * (y_a-y_p)

    # This stands for Hessian which is the second order derivative of our objective.
    # You can also think of this as the derivative of our gradient
    # np.Repeat is a numpy method used to duplicate the elements of an array
    # In this case two is getting duplicated equivalent to y_a.shape[0]
    # Run the demonstration right below to see what this does
    hess = np.repeat(2,y_a.shape[0])
    return grad, hess 
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: AvZd7KR8Zcbw
outputId: da32e38c-2e6b-47e9-bf44-91301dc53681
---
params = {
    "objective": mse,
    "verbosity": 0
}

# ** is associated with kwargs
# Has a lot of funtionailty one of which is that it can pass in a dynamic amount of parameters when used in a function.
# In our case we use it to pass a dictionary into another dictionary. This is only possible in Python 3.
model = XGBRegressor(**params)

model.fit(x_train, y_train)
predictions = model.predict(x_test)
r2 = r2_score(y_test, predictions)
print(f"r2_score: {r2 * 100.0:.2f}%")
rmse = np.sqrt(mean_squared_error(y_test, predictions))
print(f"root_mean_squared_error:{rmse}")
```

+++ {"id": "CbW3CqTBaYeU"}

###Section 3.2.1 Breaking Down the Math Behind Objective Functions
MSE Equation \\
$$C = \frac{1}{N}\sum_{i=1}^{N}(\hat{y_i}-y_i)^2$$
Please refer to slides for a more indepth derivative of this.

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 100
id: 9wQaLCfTb7HH
outputId: 1d52e25d-176d-44e7-e710-43f8152d3f10
---
# Latexify will display the the functions with a latex format
mse
```

```{code-cell}
:id: p8n4CUhhZDPs

# Dimensions of an array m * n
# Number of rows in a matrix
m = 1000
# Number of columns in a matrix
n = 500
def matrix(m, n):
    return np.zeros([m, n])
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: k9-gDNWRoOED
outputId: 2a03603c-f60e-4b1c-ce87-417b935fc1c7
---
y_p = matrix(m,n)
print(y_p)
print(y_p.shape)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: fX5UJEzLqQ3m
outputId: 2adc8747-83f2-45c5-e611-8d522b2c0521
---
y_a = np.full((m,n), 2)
print(y_a)
print(y_a.shape)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: K5cZJ-uPsI4l
outputId: edf88e84-5354-4ae0-a052-c6a4c3ace9c2
---
gradient = 2 * (y_a - y_p)
print(gradient)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: zFv1xaDD1jpH
outputId: c2c68049-adf1-4280-8f87-b2b47360290b
---
print(y_a.shape[0])
hessian = np.repeat(2,y_a.shape[0])
print(hessian.shape)
print(hessian[0:5])
```

+++ {"id": "WtEmy1xa7uot"}

##Section 3.3: Other Parameters - Learning Rate, Tree Depth, Number of Trees, Gamma and Lambda

* **learning_rate**
    * We also call it eta.
    * It is the step size shrinkage used in update to prevent overfitting.
    * eta shrinks the feature weights to make the boosting process more conservative.
    * It makes the model more robust by shrinking the weights on each step.
* **max_depth**
    * The maximum depth of a tree, same as GBM.
    * It is used to control over-fitting as higher depth will allow model to learn relations very specific to a particular sample.
    * Increasing this value will make the model more complex and more likely to overfit.
    * The deeper the tree the more memory you will consume.
* **num_estimators**
    * Number of trees to be used
    * You'll see the same parameter for Random Forest which is another esemble method using multiple decision trees. 
* **gamma**
    * A node is split only when the resulting split gives a positive reduction in the loss function.
* **lambda**
    * L2 regularization term on weights (analogous to Ridge regression).
    * This is used to handle the regularization part of XGBoost.
    * Increasing this value will make model more conservative.

+++ {"id": "5npFApQuJ3Uu"}

# Section 4

1. Example 4 - Optimizing Parameters with Bayesian Optimization
2. Example 5 - Optimizing Parameters with Cross Validation and GridSearch

+++ {"id": "Ac6PemR3h5Zy"}

## Section 4.1: Example 4 -  Optimizing Parameters with Bayesian Optimization

Baynesian Optimization is a popular method for tuning parameters.
This is the least intensive on your hardware, so if you have limited computational resources and generally get the best parameters quickly then use this.

```{code-cell}
:id: XcgBxeUOh4b6

# Exact same model we used in Section 2.1
df = pd.read_csv(f"{RAW_DATA_DIR}/pima_diabetes/diabetes.csv")
X = df.T[df.columns != "Outcome"].T
Y = df.T[df.columns == "Outcome"].T
seed = 10
test_size = 0.2
x_train, x_test, y_train, y_test = train_test_split(X,Y.values.ravel(),test_size=test_size, random_state=seed)
```

```{code-cell}
:id: sRBF1NrMi_jg

# The following is how Hyperopt evaluates the hyperparameter in each domain space
# choice: categorical variables
# uniform: continuous uniform (floats spaced evenly) between the parameter 2 and 3
# quniform: discrete uniform (integers spaced evenly) instaed returns a value from this rangeround(uniform(low, high) / q) * q

space = { "max_depth": hp.choice("max_depth", np.arange(1, 14, dtype=int)),
          "gamma": hp.uniform ("gamma", 1,9),
          "learning_rate": hp.quniform("learning_rate", 0.025, 0.5, 0.025),
          "reg_lambda": hp.uniform("reg_lambda", 0,10),
          "n_estimators":hp.choice("n_estimators", np.arange(100, 1000, 10, dtype=int)),
          "subsample": hp.quniform("subsample", 0.5, 1, 0.05)
    }
```

```{code-cell}
:id: Ju6mAsGGlE44

def objective(params):
    model = XGBClassifier(**params)
    model.fit(x_train, y_train, eval_set=[(x_train, y_train), (x_test, y_test)], eval_metric="auc", verbose=False, early_stopping_rounds=10)
    
    predictions = model.predict(x_test)
    accuracy = accuracy_score(y_test, predictions)
    # If you want to see every single message line feel free to uncomment this
    # print("Accuracy: %.2f%%" % (accuracy * 100.0))
    return {'loss': -1 * accuracy, 'status': STATUS_OK }
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 398
id: lMX5EBFxoieW
outputId: 5a8765ab-9a3b-41c3-bf6f-e8eb5ee38b43
---
# Takes over an hour if you do it with 10000 evals and the loss stays at -0.8117
# Inconsistent with my results, since sometimes I hit -0.83. Luck of the draw I guess?
trials = Trials()

best_hyperparams = fmin(fn = objective,
                        space = space,
                        algo = tpe.suggest,
                        max_evals = 10000,
                        trials = trials)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: sbJ7gM9p2ww1
outputId: c30fc03d-3d8d-43ad-9735-bc97d6267f3e
---
print("----------------- Best Hyperparameters -----------------","\n")
print(best_hyperparams)
# Test 1: {'gamma': 2.812140832501425, 'learning_rate': 0.1, 'max_depth': 10, 'n_estimators': 31, 'reg_lambda': 2.929626632845134, 'subsample': 0.8} -> 81.17% accuracy
```

+++ {"id": "F5ewtYULJ3X0"}

## Section 4.2: Example 5 -  Optimizing Parameters with Cross Validation and GridSearch
Grid Search allows you to search every option that you declare, even those that aren't promising. Although computationally taxing you won't miss the optimal point. 

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: IKpCGXlKaKNv
outputId: 032de755-7ce6-4b5b-f760-0f36c91018aa
---
# Two hours and still no results, should probably reduce the parameters
variable_params = {"n_estimators": range(100,500,100),
                   "learning_rate": [0.01, 0.1, 0.2],
                   "max_depth": range(3,13,1),
                   "subsample": [0.9, 1.0],
                   "gamma": [0, 0.25, 1.0],
                   "reg_lambda": [0, 1.0, 10.0]
                   }
static_params = {"objective": 'binary:logistic', 
                 "booster": 'gbtree',
                 }
model = XGBClassifier(**static_params)

# StratifiedKFold is based off of kfold which is a cross-validation method that divides the data into k folds.
# Stratified ensures that each fold has the same proportion of observations of a given class, whereas kfold does not.
# Cross Validation will resample your data to test and train a model on different iterations. This is used to reduced overfitting or selection bias. 
# 1.) A model is split into k smaller sets 
# 2.) A model is then trained using k-1 of those folds as training data
# 3.) The final model is validated on the remaining data
# 4.) Finally, if you set multiple folds the results are averaged across. 
kfold = StratifiedKFold(n_splits=10, shuffle=True, random_state=7)

# Grid search is a process that searches exhaustively through a manually specified subset of the hyperparameter space of the targeted algorithm.
grid_search = GridSearchCV(model, param_grid=variable_params, scoring="accuracy", n_jobs=-1, cv=kfold, verbose=1)
grid_result = grid_search.fit(X, Y.values.ravel())

# summarize results
print(f"Best: {grid_result.best_score_:.4} using {grid_result.best_params_}")

# ----------TEST 1----------
# Fitting 10 folds for each of 1728 candidates, totalling 17280 fits
# Test 1: Best: 0.7597 using {'gamma': 1.0, 'learning_rate': 0.01, 'max_depth': 5, 'n_estimators': 400, 'reg_lambda': 10.0, 'subsample': 0.9}

# ----------TEST 2----------
# Fitting 10 folds for each of 2160 candidates, totalling 21600 fits
# Best: 0.776 using {'gamma': 0, 'learning_rate': 0.01, 'max_depth': 8, 'n_estimators': 100, 'reg_lambda': 0, 'subsample': 0.9}
```

+++ {"id": "o66dP4JWQyXG"}

# Section 5

1. The Math Behind XGBoost

+++ {"id": "7-CtO9chRCWX"}

## Section 5.1: The Math Behind XGBoost
Please refer to the slides

+++ {"id": "wRgvgqo4QbDI"}

# Section 6

1. Example 6 - Fully Optimized Model
2. Example 7 - Evaluating our Model

+++ {"id": "6X2tkU0nkG4j"}

## Section 6.1: Example 6 - Fully Optimized Model 

```{code-cell}
:id: WI3AFyB6gk4z

# Parameters before and after tuning them. Keep in mind that if a parameter isn't listed in tuned_param it will just be the default
default_params = {"max_depth": 3, 
                  "learning_rate": 0.1, 
                  "n_estimators": 100, 
                  "silent": True, 
                  "objective": 'binary:logistic', 
                  "booster": 'gbtree', 
                  "n_jobs": 1, 
                  "nthread": -1, 
                  "gamma": 0, 
                  "min_child_weight": 1, 
                  "max_delta_step": 0, 
                  "subsample": 1, 
                  "colsample_bytree": 1, 
                  "colsample_bylevel": 1,
                  "reg_alpha": 0, 
                  "reg_lambda": 1, 
                  "scale_pos_weight": 1, 
                  "base_score": 0.5, 
                  "random_state": 0, 
                  "seed": None, 
                  "missing": None
                  }


tuned_param = {'gamma': 0, 
               'learning_rate': 0.01, 
               'max_depth': 8, 
               'n_estimators': 100, 
               'reg_lambda': 0, 
               'subsample': 0.9
               }
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
id: O3WVUhfUSkGf
outputId: 1fcaada3-de49-4027-a372-a7320da44b82
---
# Exact same model we used in Section 2.1
df = pd.read_csv(f"{RAW_DATA_DIR}/pima_diabetes/diabetes.csv")
X = df.T[df.columns != "Outcome"].T
Y = df.T[df.columns == "Outcome"].T
seed = 10
test_size = 0.2
x_train, x_test, y_train, y_test = train_test_split(X,Y.values.ravel(),test_size=test_size, random_state=seed)
final_model = XGBClassifier(**tuned_param)

final_model.fit(x_train, y_train.ravel(), verbose=True)
predictions = final_model.predict(x_test)
accuracy = accuracy_score(y_test, predictions)
print(f"Accuracy: {accuracy * 100.0:.2f}%") 
# Base: Accuracy: 76.62%
# Tuned Accuracy: 77.27%
```

+++ {"id": "P4lT14-ckG68"}

## Section 6.2: Example 7 - Evaluating Our Model

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 353
id: MEjyMYLBfO3s
outputId: fa03a070-22b1-403d-8861-252672996690
---
# Confusion Matrix
plot_confusion_matrix(final_model, x_test, y_test, values_format="d", display_labels=["Diabetic", "Not Diabetic"])
```

+++ {"id": "L6TbKSeI01wu"}

**Confusion Matrix Interpretation**
* We clearly see that the model is much better at predicting diabetic pima-indians than non-diabetic ones. If you recall this is becuase our data is inblalanced with more diabetic than non-diabetic results. 
* To address for this we can instead tune the scale_pos_weight parameter and use auc for evaluation or manually get rid of data.
* Only 35 (59%) of the Non-Diabetic individuals were correctly predicted
* In Contrast, 84 (88%) of the Diabetic individuals were correctly predicted

```{code-cell}
:id: iDEEWCpqSnP7

# Evaluating our model with other metrics
# Play with using the final_model and original model to see the differences
eval_set = [(x_train, y_train),(x_test, y_test)]
final_model.fit(x_train, y_train, eval_metric=["error", "logloss"], eval_set=eval_set, verbose=False)
predictions = final_model.predict(x_test)
```

```{code-cell}
:id: 0xwaOSGlUwky

# retrieve performance metrics
results = final_model.evals_result()
epochs = len(results['validation_0']['error'])
x_axis = range(0, epochs)
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 281
id: blxqhXvUVEPm
outputId: efc4040e-86f6-4af0-e174-67f760272095
---
# plot log loss
fig, ax = plt.subplots()
ax.plot(x_axis, results['validation_0']['logloss'], label='Train')
ax.plot(x_axis, results['validation_1']['logloss'], label='Test')
ax.legend()
plt.ylabel('Log Loss')
plt.title('XGBoost Log Loss')
plt.show()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 281
id: -8PTaOVuVESC
outputId: 6381c84d-d632-4a4f-8653-79214a580fbd
---
# plot classification error
fig, ax = plt.subplots()
ax.plot(x_axis, results['validation_0']['error'], label='Train')
ax.plot(x_axis, results['validation_1']['error'], label='Test')
ax.legend()
plt.ylabel('Classification Error')
plt.title('XGBoost Classification Error')
plt.show()
```

```{code-cell}
---
colab:
  base_uri: https://localhost:8080/
  height: 295
id: hA63EJHj99wy
outputId: 518b563f-83d5-40bd-b84f-075545b93573
---
# Finally lets look at feature importance
plot_importance(final_model)
plt.show()
```

+++ {"id": "BtAqA_Y83-v3"}

#**IN CONCLUSION**

* Loaded multiple datasets using the Kaggle API and setup our project directory structure for it
* Went through the basic intuition behind XGBoost and Gradient Boosting
* Did examples for both Regression and Classification datasets 
* Learned about some parameters for XGBoost
* Mathematically replicated the objective function
* Went over parameter optimization using two methods
* Optimized out XGBoost parameters using Cross Validation and GridSearch()
* Went over the math behind the XGBoost Model
* Interpreted our Optimized XGBoost Model

+++ {"id": "t3_tLN9VuLHQ"}

# Section 7

1. Example 8 - Multi-Model Evaluation

+++ {"id": "AXszLgtc74rl"}

## Section 7.1: Example 8 - Multi-Model Evaluation

```{code-cell}
:id: 5EOcSfk3pjAh

!kaggle datasets download -d uciml/breast-cancer-wisconsin-data
!mkdir -p "$ZIP_DATA_DIR/breast_cancer"
!mkdir -p "$RAW_DATA_DIR/breast_cancer"


!mv breast-cancer-wisconsin-data.zip "$ZIP_DATA_DIR/breast_cancer"

# Specific directory made for you raw unzipped data
!unzip -o "$ZIP_DATA_DIR/breast_cancer/breast-cancer-wisconsin-data.zip" -d "$RAW_DATA_DIR/breast_cancer"
```

```{code-cell}
:id: V2tutk-ErvKo

df = pd.read_csv(f"{RAW_DATA_DIR}/breast_cancer/data.csv")
df.fillna(0, inplace=True)
df.head()
```

```{code-cell}
:id: YeIxi8j0sNo4

X = df[df.columns[df.columns != "diagnosis"]]
Y = df[df.columns[df.columns == "diagnosis"]]

label_encoder = LabelEncoder()
label_encoder = label_encoder.fit(Y.values.ravel())
Y_encoded = label_encoder.transform(Y.values.ravel())

seed = 10
test_size = 0.3
x_train, x_test, y_train, y_test = train_test_split(X,Y_encoded,test_size=test_size, random_state=seed)
```

```{code-cell}
:id: HrtYi_des-25

print(x_train.shape)
print(x_test.shape)
print(y_train.shape)
print(y_test.shape)
```

```{code-cell}
:id: E7EIjihsloft

def run_exps(x_train: pd.DataFrame , y_train: pd.DataFrame, x_test: pd.DataFrame, y_test: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    models = [
          ('LogReg', LogisticRegression()), 
          ('RF', RandomForestClassifier()),
          ('KNN', KNeighborsClassifier()),
          ('SVM', SVC()), 
          ('GNB', GaussianNB()),
          ('XGB', XGBClassifier())
        ]
    results = []
    names = []
    scoring = ['accuracy', 'precision_weighted', 'recall_weighted', 'f1_weighted', 'roc_auc']
    target_names = ['malignant', 'benign']
    for name, model in models:
        kfold = model_selection.KFold(n_splits=5, shuffle=True, random_state=90210)
        cv_results = model_selection.cross_validate(model, x_train, y_train, cv=kfold, scoring=scoring)
        clf = model.fit(x_train, y_train)
        y_pred = clf.predict(x_test)
        print(name)
        print(classification_report(y_test, y_pred, target_names=target_names, zero_division=0))

        results.append(cv_results)
        names.append(name)
        this_df = pd.DataFrame(cv_results)
        this_df['model'] = name
        dfs.append(this_df)
        final = pd.concat(dfs, ignore_index=True)
    return final
```

```{code-cell}
:id: KQHCWzIFoxQz

warnings.filterwarnings('ignore')
df = run_exps(x_train, y_train, x_test, y_test)
```

```{code-cell}
:id: gtIcfIAww7Ji

bootstraps = []
for model in list(set(df.model.values)):
    model_df = df.loc[df.model == model]
    bootstrap = model_df.sample(n=30, replace=True)
    bootstraps.append(bootstrap)
        
bootstrap_df = pd.concat(bootstraps, ignore_index=True)
results_long = pd.melt(bootstrap_df,id_vars=['model'],var_name='metrics', value_name='values')
time_metrics = ['fit_time','score_time'] # fit time metrics
# PERFORMANCE METRICS
results_long_nofit = results_long.loc[~results_long['metrics'].isin(time_metrics)] # get df without fit data
results_long_nofit = results_long_nofit.sort_values(by='values')
# TIME METRICS
results_long_fit = results_long.loc[results_long['metrics'].isin(time_metrics)] # df with fit data
results_long_fit = results_long_fit.sort_values(by='values')
```

```{code-cell}
:id: 9J0UoYHUxESZ

plt.figure(figsize=(10, 6))
sns.set(font_scale=2.5)
g = sns.boxplot(x="model", y="values", hue="metrics", data=results_long_nofit, palette="Set3")
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.title('Comparison of Model by Classification Metric')
plt.savefig('./benchmark_models_performance.png',dpi=300)
```
