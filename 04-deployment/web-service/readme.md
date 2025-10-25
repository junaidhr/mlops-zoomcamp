conda activate base
pip freeze | grep scikit-learn
pipenv install scikit-learn==1.6.1 flask --python=3.9
pipenv shell
PS1="> "