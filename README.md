﻿# Projet-ETL

## Algorithme Spark

### Pré-requis
- Java 11
- IntelliJ IDEAA
- Git (pour l'installation)

### Installation

Ouvrir un terminal dans le ré1Apertoire souhaité et utiliser cette commande

```
git clone -b spark https://github.com/SupMateo/Projet-ETL
```

Ensuite il faut ouvrir le projet avec IntelliJ IDEA.
Dans les configurations de lancement, il faut précisé le SDK java 11 et indiqué le main dans la classe __fr.rts.projet.etl.SparkTest__
Le projet peut ainsi être éxecuté.

### Modification du jeu de données à télécharger

Si vous souhaitez essayer un jeu de données different : dans la classe SparkTest, veuillez indiquez au constructeur de Downloader le lien du dataset souhaité (issue de ce site : https://archive.ics.uci.edu/)
