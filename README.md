# MLOPS Ynov

Repo pour le cours de MLops

Construire un pipeline de prediction sur les données DPE tertiaire, calcul 3CL, de l'Ademe

- VM azure pour faire tourner Airflow et MLflow
- DAGS Airflow pour
    - extraire les données de l'API ADEME et les stocker brutes dans une base postgresql
    - transformer les données brutes en données de training (ints)
    - regulierement entrainer un modele (challenger) et promouvoir le meilleur modele en production (champion)
    - Creer une API avec FAstAPI pour faire des predictions
    - UI streamlit de saisie des données utilisateur

L'utilisateur obtient une prediction de chaque etiquette DPE

## Notes
Le but de ce projet est de creer un pipeline de production pas de trouver le meilleur modele predictif.

Il n'y a pas d'exploration des donnees. Et fort probablement du leakage entre certaines variables (consommation) et l'etiquette DPE finale

Le modele considéré est simple: une random forest avec des variations sur la max_depth, le nombre de trees (n_estimator) et le min sample leaf.


**WIP** **WIP** **WIP** **WIP** Cette repo est un WIP, loin d'etre finalisée. C'est le souk, le bordel, le foutoir. I know.

## Simulation du drift des modele
Pour simuler le drift des données et du modele on compare a chaque nouvel entrainement du modele challenger sur un subset du dataset principal obtenu aleatoirement, la performance du champion et du challenger
sur les N echantillons les plus recents. en faisant varier aussi aleatoirement les parametres du challenger. De cette facon les perfs du challenger varient fortement en fonction des parametrs et des echantillons du subset d'entrainement.

En parallele les données sont engrangées dans la bdd de training, offrant la possibilité que les perfs du modele champion se degrade soudainement.

C'est tres hacky mais ca permet de simuler une degradation des performances du modele de production necessitant d'etre remplacé par un nouveau modele.

