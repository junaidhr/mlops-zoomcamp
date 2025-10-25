conda activate base
pip freeze | grep scikit-learn
pipenv install scikit-learn==1.6.1 flask --python=3.9
pipenv shell
PS1="> "



## Deploying a model as a web-service

* Creating a virtual environment with Pipenv
* Creating a script for predictiong 
* Putting the script into a Flask app
* Packaging the app to Docker


```bash
docker build -t ride-duration-prediction-service:v1 .
```

```bash
docker run -it --rm -p 9696:9696  ride-duration-prediction-service:v1
```