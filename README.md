
Ce projet a pour objectif de calculer et visualiser des **indicateurs de qualité** à partir d’une base de données, en utilisant une chaîne d'outils modernes et automatisés : **Apache Airflow**, **PostgreSQL**, **Superset**, le tout orchestré avec **Docker**.

---


### 1. Pré-requis

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installé
- Accès à ce dépôt (cloner ou télécharger)

### 2. Cloner le projet

```dans le terminal
git clone https://github.com/ton-pseudo/data-project.git
cd data-project

Ensuite taper :

docker compose up

Cela démarre :

Apache Airflow sur http://localhost:8080

Superset sur http://localhost:8088

PostgreSQL (service de base de données)
