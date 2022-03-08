import numpy as np
import pandas as pd

a = np.array([[11, 12, 13, 14, 15],
           [16, 17, 18, 19, 20],
           [21, 22, 23, 24, 25],
           [26, 27, 28 ,29, 30],
           [31, 32, 33, 34, 35]])

# 所有行第一列；
a[:, 1]

# 第0列[1,4)行；
a[0, 1:4] # 注意只有 3 行

# 索引值能被2整除的所有行能被2整除的所有列。
a[::2,::2]


##################################################
# Pandas cheat
##################################################

f.index.get_level_values(0).dtype
# => dtype('int64')

# rolling on multindex
df['mavg5'] =df.groupby(level='ts_code').close.rolling(window=5).mean().values
# rolling sum 这样会多一个 index
df.groupby(level='ts_code').pct_chg.rolling(window=window).sum().shift(-window)
# rolling sum 必须这样
df.groupby(level='ts_code').pct_chg.apply(lambda x: x.rolling(window=3).sum().shift(-3))


###################### Column Name Operations ################


# Lower-case all DataFrame column names
df.columns = map(str.lower, df.columns)


###################### Value Operations ######################
# List unique values in a DataFrame column
df['Column Name'].unique()

# Convert Series datatype to numeric (will error if column has non-numeric values)
pd.to_numeric(df['Column Name'])

# Convert Series datatype to numeric, changing non-numeric values to NaN
pd.to_numeric(df['Column Name'], errors='coerce')

# Delete column from DataFrame
del df['column']

# Rename several DataFrame columns
df = df.rename(columns = {
    'col1 old name':'col1 new name',
    'col2 old name':'col2 new name',
    'col3 old name':'col3 new name',
})

# 统计一行中多个列的值
df[col_list].sum(axis=1)

###################### Filters ################################
def filters():
    pass


# filter by index value1
df[df.index.isin(['Lake', 'River', 'Upland'], level=1)]
df.xs(('index level 1 value','index level 2 value'), level=('level 1','level 2'))



# Grab DataFrame rows where column = a specific value
df = df.loc[df.column == 'somevalue']

# Grab DataFrame rows where column value is present in a list
valuelist = ['value1', 'value2', 'value3']
df = df[df.column.isin(valuelist)]

# Grab DataFrame rows where column value is not present in a list
valuelist = ['value1', 'value2', 'value3']
df = df[~df.column.isin(value_list)]

# Select from DataFrame using criteria from multiple columns
# (use `|` instead of `&` to do an OR)
newdf = df[(df['column_one']>2004) & (df['column_two']==9)]

# Even more fancy DataFrame column re-naming
# lower-case all DataFrame column names (for example)
df.rename(columns=lambda x: x.split('.')[-1], inplace=True)


###################### Loop ################################
# Loop through rows in a DataFrame
# (if you must)
for index, row in df.iterrows():
    print index, row['some column']

# Much faster way to loop through DataFrame rows
# if you can work with tuples
# (h/t hughamacmullaniv)
for row in df.itertuples():
    print(row)


###########################
###
##########################
dd = x.pivot_table(values=['Volume','Amount'], 
                  columns=['order_size','Type'], 
                  index='ts_code', 
                  fill_value=0,
                  aggfunc='first')


# Next few examples show how to work with text data in Pandas.
# Full list of .str functions: http://pandas.pydata.org/pandas-docs/stable/text.html

# Slice values in a DataFrame column (aka Series)
df.column.str[0:2]

# Lower-case everything in a DataFrame column
df.column_name = df.column_name.str.lower()

# Get length of data in a DataFrame column
df.column_name.str.len()

# Sort dataframe by multiple columns
df = df.sort_values(['col1','col2','col3'],ascending=[1,1,0])

# Get top n for each group of columns in a sorted dataframe
# (make sure dataframe is sorted first)
top5 = df.groupby(['groupingcol1', 'groupingcol2']).head(5)

# Grab DataFrame rows where specific column is null/notnull
newdf = df[df['column'].isnull()]

# multi-condition where
df1 = df[(df.a != -1) & (df.b != -1)]

# Select from DataFrame using multiple keys of a hierarchical index
df.xs(('index level 1 value','index level 2 value'), level=('level 1','level 2'))

# Change all NaNs to None (useful before
# loading to a db)
df = df.where((pd.notnull(df)), None)

# More pre-db insert cleanup...make a pass through the dataframe, stripping whitespace
# from strings and changing any empty values to None
# (not especially recommended but including here b/c I had to do this in real life one time)
df = df.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

# Get quick count of rows in a DataFrame
len(df.index)

# Pivot data (with flexibility about what what
# becomes a column and what stays a row).
# Syntax works on Pandas >= .14
pd.pivot_table(
  df,values='cell_value',
  index=['col1', 'col2', 'col3'], #these stay as columns; will fail silently if any of these cols have null values
  columns=['col4']) #data values in this column become their own column

# Change data type of DataFrame column
df.column_name = df.column_name.astype(np.int64)

# Get rid of non-numeric values throughout a DataFrame:
for col in refunds.columns.values:
  refunds[col] = refunds[col].replace('[^0-9]+.-', '', regex=True)

# Set DataFrame column values based on other column values (h/t: @mlevkov)
df.loc[(df['column1'] == some_value) & (df['column2'] == some_other_value), ['column_to_change']] = new_value

# Clean up missing values in multiple DataFrame columns
df = df.fillna({
    'col1': 'missing',
    'col2': '99.999',
    'col3': '999',
    'col4': 'missing',
    'col5': 'missing',
    'col6': '99'
})

# Concatenate two DataFrame columns into a new, single column
# (useful when dealing with composite keys, for example)
# (h/t @makmanalp for improving this one!)
df['newcol'] = df['col1'].astype(str) + df['col2'].astype(str)

# Doing calculations with DataFrame columns that have missing values
# In example below, swap in 0 for df['col1'] cells that contain null
df['new_col'] = np.where(pd.isnull(df['col1']),0,df['col1']) + df['col2']

# Split delimited values in a DataFrame column into two new columns
df['new_col1'], df['new_col2'] = zip(*df['original_col'].apply(lambda x: x.split(': ', 1)))

# Collapse hierarchical column indexes
df.columns = df.columns.get_level_values(0)

# Convert Django queryset to DataFrame
qs = DjangoModelName.objects.all()
q = qs.values()
df = pd.DataFrame.from_records(q)

# Create a DataFrame from a Python dictionary
df = pd.DataFrame(list(a_dictionary.items()), columns = ['column1', 'column2'])

# Get a report of all duplicate records in a dataframe, based on specific columns
dupes = df[df.duplicated(['col1', 'col2', 'col3'], keep=False)]

# Set up formatting so larger numbers aren't displayed in scientific notation (h/t @thecapacity)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# To display with commas and no decimals
pd.options.display.float_format = '{:,.0f}'.format


# suppress scientific notation
dfm.uv.describe().apply(lambda x: format(x, 'f'))
# List unique values in a DataFrame column
# h/t @makmanalp for the updated syntax!
df['Column Name'].unique()

# Convert Series datatype to numeric (will error if column has non-numeric values)
# h/t @makmanalp
pd.to_numeric(df['Column Name'])

# Convert Series datatype to numeric, changing non-numeric values to NaN
# h/t @makmanalp for the updated syntax!
pd.to_numeric(df['Column Name'], errors='coerce')

# Grab DataFrame rows where column = a specific value
df = df.loc[df.column == 'somevalue']

# Grab DataFrame rows where column value is present in a list
valuelist = ['value1', 'value2', 'value3']
df = df[df.column.isin(valuelist)]

# Grab DataFrame rows where column value is not present in a list
valuelist = ['value1', 'value2', 'value3']
df = df[~df.column.isin(value_list)]

# Delete column from DataFrame
del df['column']

# Select from DataFrame using criteria from multiple columns
# (use `|` instead of `&` to do an OR)
newdf = df[(df['column_one']>2004) & (df['column_two']==9)]

# Rename several DataFrame columns
df = df.rename(columns = {
    'col1 old name':'col1 new name',
    'col2 old name':'col2 new name',
    'col3 old name':'col3 new name',
})

# Lower-case all DataFrame column names
df.columns = map(str.lower, df.columns)

# Even more fancy DataFrame column re-naming
# lower-case all DataFrame column names (for example)
df.rename(columns=lambda x: x.split('.')[-1], inplace=True)

# Loop through rows in a DataFrame
# (if you must)
for index, row in df.iterrows():
    print index, row['some column']

# Much faster way to loop through DataFrame rows
# if you can work with tuples
# (h/t hughamacmullaniv)
for row in df.itertuples():
    print(row)

# Next few examples show how to work with text data in Pandas.
# Full list of .str functions: http://pandas.pydata.org/pandas-docs/stable/text.html

# Slice values in a DataFrame column (aka Series)
df.column.str[0:2]

# Lower-case everything in a DataFrame column
df.column_name = df.column_name.str.lower()

# Get length of data in a DataFrame column
df.column_name.str.len()

# Sort dataframe by multiple columns
df = df.sort_values(['col1','col2','col3'],ascending=[1,1,0])

# Get top n for each group of columns in a sorted dataframe
# (make sure dataframe is sorted first)
top5 = df.groupby(['groupingcol1', 'groupingcol2']).head(5)

# Grab DataFrame rows where specific column is null/notnull
newdf = df[df['column'].isnull()]

# multi-condition where
df1 = df[(df.a != -1) & (df.b != -1)]

# Select from DataFrame using multiple keys of a hierarchical index
df.xs(('index level 1 value','index level 2 value'), level=('level 1','level 2'))

# Change all NaNs to None (useful before
# loading to a db)
df = df.where((pd.notnull(df)), None)

# More pre-db insert cleanup...make a pass through the dataframe, stripping whitespace
# from strings and changing any empty values to None
# (not especially recommended but including here b/c I had to do this in real life one time)
df = df.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

# Get quick count of rows in a DataFrame
len(df.index)

# Pivot data (with flexibility about what what
# becomes a column and what stays a row).
# Syntax works on Pandas >= .14
pd.pivot_table(
  df,values='cell_value',
  index=['col1', 'col2', 'col3'], #these stay as columns; will fail silently if any of these cols have null values
  columns=['col4']) #data values in this column become their own column

# Change data type of DataFrame column
df.column_name = df.column_name.astype(np.int64)

# Get rid of non-numeric values throughout a DataFrame:
for col in refunds.columns.values:
  refunds[col] = refunds[col].replace('[^0-9]+.-', '', regex=True)

# Set DataFrame column values based on other column values (h/t: @mlevkov)
df.loc[(df['column1'] == some_value) & (df['column2'] == some_other_value), ['column_to_change']] = new_value

# Clean up missing values in multiple DataFrame columns
df = df.fillna({
    'col1': 'missing',
    'col2': '99.999',
    'col3': '999',
    'col4': 'missing',
    'col5': 'missing',
    'col6': '99'
})

# Concatenate two DataFrame columns into a new, single column
# (useful when dealing with composite keys, for example)
# (h/t @makmanalp for improving this one!)
df['newcol'] = df['col1'].astype(str) + df['col2'].astype(str)

# Doing calculations with DataFrame columns that have missing values
# In example below, swap in 0 for df['col1'] cells that contain null
df['new_col'] = np.where(pd.isnull(df['col1']),0,df['col1']) + df['col2']

# Split delimited values in a DataFrame column into two new columns
df['new_col1'], df['new_col2'] = zip(*df['original_col'].apply(lambda x: x.split(': ', 1)))

# Collapse hierarchical column indexes
df.columns = df.columns.get_level_values(0)

# Convert Django queryset to DataFrame
qs = DjangoModelName.objects.all()
q = qs.values()
df = pd.DataFrame.from_records(q)

# Create a DataFrame from a Python dictionary
df = pd.DataFrame(list(a_dictionary.items()), columns = ['column1', 'column2'])

# Get a report of all duplicate records in a dataframe, based on specific columns
dupes = df[df.duplicated(['col1', 'col2', 'col3'], keep=False)]

# Set up formatting so larger numbers aren't displayed in scientific notation (h/t @thecapacity)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# To display with commas and no decimals
pd.options.display.float_format = '{:,.0f}'.format


# suppress scientific notation
dfm.uv.describe().apply(lambda x: format(x, 'f'))
