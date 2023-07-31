import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import io
import boto3

AWS_S3_BUCKET = "hranalyticsdata"
AWS_ACCESS_KEY_ID = "AKIAW2XDBWNOSRSAYPUT"
AWS_SECRET_ACCESS_KEY  = "Zkxsi3fif/vTn/Zw1Xn8OOOSnJ69lJ//6UAqa1IN"



class Transformation:
    def __init__(self, df):
        self.df = df
        
    def replace_enrolled_university(self):
        uni_miss = self.df.loc[self.df['enrolled_university'].isna()]
        uni_miss['enrolled_university'] = self.df.loc[self.df['enrolled_university'].notna()]['enrolled_university'].mode()[0]
        self.df = pd.concat([self.df.loc[self.df['enrolled_university'].notna()], uni_miss], ignore_index=True)
    
    def replace_gender(self):
        gender_miss = self.df.loc[self.df['gender'].isna()]
        gender_miss['gender'] = 'Other'
        self.df = pd.concat([self.df.loc[self.df['gender'].notna()], gender_miss], ignore_index=True) 
    
    def replace_major_discipline(self):
        maj_disc_miss = self.df.loc[self.df['major_discipline'].isna()]
        maj_disc_miss['major_discipline'] = 'Other'
        self.df = pd.concat([self.df.loc[self.df['major_discipline'].notna()], maj_disc_miss], ignore_index=True)       
        
    def replace_education_level(self):
        edu_miss = self.df.loc[self.df['education_level'].isna()]
        edu_miss['education_level'] = self.df.loc[self.df['education_level'].notna()]['education_level'].mode()[0]
        self.df = pd.concat([self.df.loc[self.df['education_level'].notna()],edu_miss], ignore_index=True)
    
    def replace_last_new_job(self):
        job_miss  = self.df.loc[self.df['last_new_job'].isna()]
        job_miss['last_new_job'] = self.df.loc[self.df['last_new_job'].notna()]['last_new_job'].mode()[0]
        self.df = pd.concat([self.df.loc[self.df['last_new_job'].notna()],job_miss], ignore_index=True)
    
    def replace_company_type(self):
        type_miss = self.df.loc[self.df['company_type'].isna()]
        type_miss['company_type'] = 'Other'
        self.df = pd.concat([self.df.loc[self.df['company_type'].notna()], type_miss], ignore_index=True)
    
    def replace_company_size(self):
        size_miss = self.df.loc[self.df['company_size'].isna()]
        size_miss['company_size'] = 'Other'
        self.df = pd.concat([self.df.loc[self.df['company_size'].notna()], size_miss], ignore_index=True)
    
    def replace_experience(self):
        exp_miss = self.df.loc[self.df['experience'].isna()]
        exp_miss['experience'] = self.df.loc[self.df['experience'].notna()]['experience'].mode()[0]
        self.df = pd.concat([self.df.loc[self.df['experience'].notna()], exp_miss], ignore_index=True) 
        
    
    def run_process(self):
        self.replace_enrolled_university()
        self.replace_gender()
        self.replace_major_discipline()
        self.replace_education_level()
        self.replace_last_new_job()
        self.replace_company_type()
        self.replace_company_size()
        self.replace_experience()
        
        return self.df
    
    
    
def read_from_s3(filename,key,ext='csv'):
    s3_client = boto3.client(
                            "s3",
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            )
    filename = f"{key}/{filename}.{ext}"
    obj = s3_client.get_object(Bucket = AWS_S3_BUCKET,Key = filename)
    data=obj.get('Body').read()
    df = pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)
    
    return df

def write_to_s3(df, final_df_name,key, ext= 'csv'):
    s3_client = boto3.client(
                            "s3",
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            )
    
    key = f"{key}/{final_df_name}.{ext}"
    print(key)
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=key, Body=csv_buffer.getvalue()
            )
        print(response)   
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
            

def labeled_barplot(data, feature, perc=False, n=None):

    total = len(data[feature])  
    count = data[feature].nunique()
    if n is None:
        plt.figure(figsize=(count + 1, 5))
    else:
        plt.figure(figsize=(n + 1, 5))

    plt.xticks(rotation=90, fontsize=15)
    ax = sns.countplot(
        data=data,
        x=feature,
        palette="Paired",
        order=data[feature].value_counts().index[:n].sort_values(),
    )

    for p in ax.patches:
        if perc == True:
            label = "{:.1f}%".format(
                100 * p.get_height() / total
            ) 
        else:
            label = p.get_height() 

        x = p.get_x() + p.get_width() / 2  
        y = p.get_height()  

        ax.annotate(
            label,
            (x, y),
            ha="center",
            va="center",
            size=12,
            xytext=(0, 5),
            textcoords="offset points",
        )  

    plt.show()