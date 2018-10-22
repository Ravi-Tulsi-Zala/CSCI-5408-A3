import pandas as pd
import re
import csv
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LogisticRegression
import pyspark
from pyspark.sql.session import SparkSession
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score
from sklearn import model_selection
from sklearn.externals import joblib
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix


# Read the Tweets.csv file with available "label training data"
df=pd.read_csv('Tweets.csv',header=0)

# Select the first 2000 rows
df.drop(df.index[2000:],inplace=True)

# Remove the unnecessary columns to increase the efficiency
df.drop(df.columns[[2,3,4,5,6,7,8,9,11,12,13,14]],axis=1,inplace=True)
df.to_csv("TweetsData.csv")

# Creating the data frame object for required columns
df_text = df['text']
df_id = df['tweet_id']

# Converting the data which is compatible to Logistic Regression ["Feature Extraction"]
sentiment = pd.get_dummies(df['airline_sentiment'])
sentiment.drop(sentiment.columns[[1,2]],axis=1,inplace=True)

# New column added to the data frame object which is used in Logistic Regression
sentiment.rename(columns={'negative':'sentiment_label'},inplace=True)

#empty lists for storing tweet_ids and tokenized tweet text
cleaned_list = []
tweet_id = []

# Cleaning the available tweets and removing the redundant data
for tweet in df_text:
    tweet=tweet.split()
    for i in range(len(tweet)):
        if(tweet[i].find('@') >= 0 or tweet[i].find('http') >= 0 or tweet[i].find('https') >= 0):
            tweet[i]=""
    tweet = ' '.join(tweet)
    tweet = re.sub('[^\w]', ' ', tweet)
    tweet = re.sub(r'\b\w{1}\b','', str(tweet))
    tweet = re.sub(' +', ' ',tweet)
    cleaned_list.append(tweet)
    

for tweet in df_id:

    tweet_id.append(tweet)

#Writing the Tokenized_Text and tweet_id columns into new csv file
with open('Refined_Tweets.csv', 'w+',encoding="utf8") as myfile:

    fieldnames = ['Tokenized_Text','tweet_id']
    wr = csv.DictWriter(myfile, fieldnames = fieldnames,delimiter=',')
    wr.writeheader()
    for i in range(len(cleaned_list)):
        wr.writerow({'Tokenized_Text':cleaned_list[i],'tweet_id':tweet_id[i]})


# Instantiate the spark variable with spark session
spark = SparkSession.builder.appName('pipe').getOrCreate()

# Creating the spark data frame and loading the cleaned tweet file into that data frame
spark_df=spark.read.csv('Refined_Tweets.csv',header=True)

# Coverting the spark data frame into pandas data frame object
tokenized_df = spark_df.toPandas()

#As the types of Tokenizd dataframe's tweet_id and main dataframe's tweet_id is different , type casting is needed
tokenized_df['tweet_id'] = tokenized_df['tweet_id'].apply(int)

final = pd.merge(tokenized_df,df[['tweet_id','text','airline_sentiment']],on='tweet_id')
# Making the final data frame object to work with clean tweets and sentiment labels for the model Logistic Regression
final_df = pd.concat([final,sentiment], axis = 1)


token_list =[]
df_tokens = final['Tokenized_Text']

for token in df_tokens:
    token = token.lower()
    token_list.append(token)

# Feature Extraction for clean tweets
cv = CountVectorizer()
X = cv.fit_transform(token_list)

# Output variable to predict
Y=final_df['sentiment_label']

# Divide the data set into test and train data
X_train, X_test, y_train, y_test=train_test_split(X,Y,test_size=0.1,random_state=1)

# Initialize the Logisitic Regression model
lr=LogisticRegression()
lr.fit(X_train,y_train)

# Perform the prediction on test data set
predictions=lr.predict(X_test)

# Initialize the pipeline object and save that for future use
pipe='pipe.sav'
pipeline=joblib.dump(lr,pipe)

# Load the pipeline and predict the score on the new data
load = joblib.load(pipe)
new_res = load.score(X_test,y_test)

print('\n')
print("Score after loading it into Pipeline : "+ str(new_res))
print('\n')
print("---------------------------------------------------")
print("Classification Report : ")

#Classification Report with precision score prediction
print(classification_report(y_test,predictions))

print("----------------------------------------------------")
print('\n')
#Accuracy measurement of this model
print("Accuracy score of Model : "+ str(accuracy_score(y_test,predictions)))
print('\n')
print("----------------------------------------------------")
print("Confusion Matrix : ")

#Performance of a classifier using True Positive,False Negative in first row of matrix.. False Positive,True Negative in second row
print(confusion_matrix(y_test,predictions))
print("----------------------------------------------------")


""""
Score after loading it into Pipeline : 0.8


---------------------------------------------------
Classification Report : 
             precision    recall  f1-score   support

          0       0.69      0.77      0.73        69
          1       0.87      0.82      0.84       131

avg / total       0.81      0.80      0.80       200

----------------------------------------------------


Accuracy score of Model : 0.8


----------------------------------------------------
Confusion Matrix : 
[[ 53  16]
 [ 24 107]]
----------------------------------------------------


"""