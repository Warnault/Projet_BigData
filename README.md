# Projet_BigData


#### . Le projet doit s'exécuter sur le cluster de salle 203. Afin de configurer votre espace de travail correctement il faut lancer la commande :
  - ``` source /espace/Auber_PLE-203/user-env.sh ```
 
#### . Pour exécuter le code dans un premier temps, il faut générer les .class, pour cela vous devez compiler avec maven : 
  -  ``` mvn compile ; mvn package ```
 
#### . Pour exécuter un MapReduce il faut lancer la commande :
  -```hdfs dfs -rm -r -f hdfs://data:9000/user/<votre_espace_de_travaille_sur_le_cluster>/resultat ; yarn jar target/ple-twitter-project-0.0.1.jar <ARG_1> <ARG_2> <PARAMS> <fichier_de_data.nljson> <dossier_pour_save_le_resultat> ```
  
attention ARG2 depent de ARG1.

##### ARG1 peut-etre :
  - ```User``` pour faire des recherches sur les utilisateurs,
  - ``` Hashtag``` pour faire des recherches sur les hashtag.
  
##### ARG2 :
  - Pour l'arg1 User vous pouvez utilser :
    - ``` c ``` pour récupérer la liste des pays d'où viennent les tweets, pas de ```<PARAMS>```;
    - ``` l ```  pour récupérer la liste des langues dans la quelle sont ecrits les tweets, pas de ```<PARAMS>```;
    - ``` h ```  pour récupérer la liste des hashtags utliser pas un utilisateur dans ses tweets,  ```<PARAMS>``` doit être égale au pseudo de l'utilisateur(sceen_name si vous le rechercher dans la base de données) ;
    - ```cpt_tweet ```  pour récupérer le nombre de tweets d'un utilisateur,  ```<PARAMS>``` doit être égale au pseudo de l'utilisateur (sceen_name si vous le rechercher dans la base de donnees) ;
  - Pour l'arg1 Hashtag vous pouvez utiliser :
    - ```l```  pour récupérer la liste de tout les hashtags du fichier, pas de ```<PARAMS>```;
    - ```topk_freq``` pour récupérer la liste de tout les n hashtags les plus utlisés, ```<PARAMS>``` doit être égale au n choisi ;
    - ```u``` pour récupérer la liste utilisateur ayant utilisé  l'hashtags,```<PARAMS>``` doit être égale au nom du hastag choisi ;
    - ```f``` pour récupérer le nombre d'apparition d'un hashtag donné, ```<PARAMS>``` doit être égale au nom du hastag choisi ;
  
  
