# dscc202-402-spring2023
Course Materials for Data Science at Scale

## Establishing a GitHub account if your do not have one already
[Sign up for a new github account](https://docs.github.com/en/github/getting-started-with-github/signing-up-for-a-new-github-account) <br>

## Fork the class repository into your account
Fork the dsc402 repositiory into your new account.  Note: this will create a copy of the course repo for you to add and work on within your
own account.<br>
Goto https://github.com/lpalum/dscc202-402-spring2023 and hit the fork button while you are logged into your github account: ![fork image](https://github-images.s3.amazonaws.com/help/bootcamp/Bootcamp-Fork.png)

## Clone your copy of the dsc402 repository to get the class materials on your machine
<code>git clone https://github.com/[your account name]/dscc202-402-spring2023.git</code><br>
note: you may want to clone this repo into a dirtory on your machine that you organize for code e.g. **/home/<your username>/code/github**

note: **/home/[your account name] should be /Users/[your account name] to work with the paths that are defined in Mac OS X.**

## Sign-up for the Community Edition of Databricks and import archives
[Databrick Community Edition FAQ](https://databricks.com/product/faq/community-edition)
  
Note: you will also be receiving an email invite to the class shared Databricks Workspace which is where you will be doing your final project.

Here is some helpful information about importing archives into to the Databricks Envioronment: 
https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook

import the DBC archive from the Learning Spark v2 github repositiory into your account. (this is the code that goes along with the test book)
[DBC Archive](https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc)

Import the DBC archives for the class after unzipping dscc202-402-dbc.zip on your machine.
- asp.dbc   apache spark programming
- de.dbc    data engineering with delta lake and spark
- ml.dbc    machine learning on spark

## Running your own local Spark Environment on your Computer [optional]
[Install docker on your computer](https://docs.docker.com/get-docker/)

Pull the all-spark-notebook image from docker hub: <br>
<code>https://hub.docker.com/r/lpalum/dsc402</code>
<br>Launch the docker image to open a Jupyter Lab instance in your local browser:<br>
<code>docker run -it --rm -p 8888:8888 --name all-spark --volume /home/[your account name]/code/github:/home/jovyan/work lpalum/dsc402 start.sh jupyter lab</code>

This will start a jupyter lab instance on your machine that you will be able to access at port 8888 in your browser and it will mount the github repo that you previouly
cloned into the containers working directory.

