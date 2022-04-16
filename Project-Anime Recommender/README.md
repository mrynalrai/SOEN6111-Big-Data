## Project definition:
<p align="justify">
In today's age of technological advancements,capturing and storing user interactions electronically has provided an opportunity to study socio-economic patterns and build data driven platforms. Recommendations System is one such platform that uses data and recommends/predicts user's possible future likes and interest areas. 
</p>
<p>
Studying & building a recommendation system for Anime Data-set(Media domain) has particularly caught the attention for our project. Anime originated in Japan and gradually gained rapid traction across the global market leading to huge financial investments and technological advancements. The global anime market size reached at US$ 22.6 billion in 2020 is predicted to be worth around US$ 48.3 billion by 2030. Our Project outline is to study & develop an anime recommendation system that provides personalized anime recommendations to its users for various Online video streaming platforms.
</p>

## Model design:
<p align="justify">
Anime Data model consists of five csv files such as animelist, rating_complete, anime_with_synopsis, anime and watching_status. User ratings across anime’s is present in animelist along with implicit user metrics like watching status & episode watch count. User ratings form an essential metric in designing our data model. The animelist file includes 0.3M unique users who rated ~16.5K unique anime with score 0 to 10. 
</p>
<p>
Anime File includes various categorical metrics like name, genre, producer, studio, source etc. These categorical metrics will form the basis for discovering relevant factors using dimensionality reduction techniques such as PCA. EDA key challenges in designing the data model are as follows : unpivot user rated score columns, unpivot  anime feature columns, split comma separated genres, missing value treated for "unknown scores", handling inconsistent date metric etc. EDA Analysis such as Top/Bottom 10 most rated movies where user count > mean user count who have rated the movies, Top/Bottom movies across genre, user ratings distribution(mean,max,mean,median), frequency distribution across user rating & anime  etc was completed for better understanding, thus helping in solving designing concerns for our data model.
</p>
 

## Research Area:
<p align="justify">
1. Study and develop an anime recommender system that provides personalized anime recommendation for its user. </p>
<p align="justify">
2. Research, study and compare multiple recommender system models for better understanding of recommendation systems. </p>

## Algorithm:
<p align="justify">
    We will be creating a personalized anime recommendation using explicit data set from anime. </p>
  <p>
  	Our objective is to study and develop KNN item-based collaborative filtering with Pearson correlation coefficient to recommend Top N animes to users. We will compare the above method results with another technique called Matrix Factorization(ALS) which utilizes latent factors to improve on limitations like cold start, overfitting , popularity bias, scalability and sparse matrix issues. Using ALS, we want to factorize rating matrices into user and anime matrices to predict better recommendation for users. The evaluation matrix for ALS method is RMSE and Top N.
</p>
<p>
  	Future Scope is to use implicit metrics like number of watched episodes with ALS to make recommendations for users.
  </p>

## Dataset
<p> https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020 </p>
<p> ratings_complete.csv (Private): https://drive.google.com/drive/folders/15WlaYzvT-xAY_CteuJRhPKQGcRE9iiZo </p>
