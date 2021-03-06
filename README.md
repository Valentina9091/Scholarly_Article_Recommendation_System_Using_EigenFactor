# Scholarly Article Recommendation System Using EigenFactor

This algorithm recommends articles based on the citation information available for each article. It is a modified version of page rank. The papers are recommended based on the hierarchical clustering and Article Level Eigen Factor scores.

DATA SET
AMiner-Paper.rar data set is the focus.
https://aminer.org/billboard/aminernetwork

This data set contains:

#index ---- index id of this paper

#* ---- paper title

#@ ---- authors (separated by semicolons)

#o ---- affiliations (separated by semicolons, and each affiliation corresponds to an author in order)

#t ---- year 

#c ---- publication venue

#% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)

#! ---- abstract


Steps:
1. Assemble Citation Network
The first step requires assembling the citation graph for the large corpus. The first column is the citing paper ID and the column 2
is the cited paper id. 
Format: 1083734 197394###220708###387427

2. Rank Node
Then we rank each article using “article level eigen factor algorithm” (ALEF). When page rank approaches are applied to acyclic citation graphs older papers are weighted excessively. ALEF is a modified version of page rank. The ALEF algorithm consists of five steps: a. The teleportation weight, wi for each node i is calculated by summing the in and out citations.

![Alt text](https://github.com/Valentina9091/Scholarly_Article_Recommendation_System_Using_EigenFactor/blob/master/img/1.png "teleportation weight")

Forming row stochastic matrix Hij:- The matrix Zij is then row normalized so that the sum of each row i equals, we call this row stochastic matrix,

![Alt text](https://github.com/Valentina9091/Scholarly_Article_Recommendation_System_Using_EigenFactor/blob/master/img/2.png " row stochastic matrix Hij")

Calculating Article Level Eigen Factor:- The ALEF scores are then calculated by multiplying wi by Hij and normalizing the scores by the number of papers, n, in the corpus

![Alt text](https://github.com/Valentina9091/Scholarly_Article_Recommendation_System_Using_EigenFactor/blob/master/img/3.png "Article Level Eigen Factor")

3. Clustering the nodes hierarchically

We cluster the nodes using Map equation API(http://www.mapequation.org/)

4. Recommendation Selection

Now we have eigen factor scores and the clusters of the nodes using Map equation. Whenever a user searches for a document, we determine the cluster in which the document is located. Then we rank those documents in that based on the eigen factor scores and display them to the user
