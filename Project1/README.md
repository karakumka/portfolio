```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from numpy.linalg import eig
import seaborn as sns
from scipy.stats import median_abs_deviation
```

```python
data = pd.read_csv('apriltaxis_sample.csv')
data.head()
```

# 0. Data Preprocessing

As PCA can only be applied to continuous data, we will store continuous variables to a separate dataset.

```python
df_continuous = data[['Trip Seconds', 'Trip Miles', 'Fare', 'Tips', 'Extras', 'Trip Total']]
```

Let's check the number of outliers and make a decision about its presence in the dataset.

```python
for column in df_continuous:
    Q3 = np.quantile(df_continuous[column], 0.75)
    Q1 = np.quantile(df_continuous[column], 0.25)
    threshold = 1.5 * (Q3 - Q1)
    outliers = sum((df_continuous[column] > Q3 + threshold) | (df_continuous[column] < Q1 - threshold))
    print(f'There are {outliers} outliers in the column {column}.')
```

The column "Extras" has more than 20% outliers, and removing them may seem too critical for the following analysis. By retaining the outliers, the results of the PCA would be able to reflect the full variability of the data, including common patterns as well as rare extreme cases.

Next, we must decide whether to work with covariance or correlation matrix in our PCA analysis. To prove the necessity of the standardization of the data, we will check if the data is similar in measurement scale and variance. Looking at the original dataframe, we notice that four of our variables are measured in dollars, one of them is measured in miles, and the last one - in seconds. Below there are the boxplots to check the variance.

```python
plt.boxplot(df_continuous, labels=df_continuous.columns)
plt.title('Boxplots for the original continuous variables')
plt.show()
```

The variances are not commensurate: the variable "Trip Seconds" measured in seconds has the biggest variance that can affect the result of the PCA. Thus, we will standardize the data and work with the correlation matrix.

```python
X = df_continuous.values
X = StandardScaler().fit_transform(X)
np.mean(X), np.std(X)  # check if correct
```
```python
plt.boxplot(X, labels=df_continuous.columns)
plt.title('Boxplots for the standardized continuous variables')
plt.show()
```

# 1. PCA
## 1.1. Select the number of principal components

First, we need to calculate the correlation matrix as our data is standardized.

```python
np.set_printoptions(precision=5)
R = np.corrcoef(X, rowvar=False)
R
```

Note that all pairwise correlations are positive.

Second, we find the eigen values and eigen vectors of the correlation matrix above.

```python
eig_vals, eig_vecs = eig(R)
print(f'Eigen values: {eig_vals}')
print(f'Eigen vectors: {eig_vecs}')
```

Spectral decomposition for the correlation matrix R has the following form:
$$ R = \tilde{T}\tilde{Λ}\tilde{T'} $$, where $\tilde{T}$ is a matrix with eigenvectors of R, $\tilde{Λ}$ is a diagonal matrix with eigen values on the diagonal, $\tilde{T'}$ is a transpose of T.

Let's manually calculate the spectral decomposition of a matrix R.

```python
A = np.zeros([6, 6])
for i in range(len(A)):
    A[i][i] = eig_vals[i]
T = eig_vecs
```

Here T is a matrix with eigenvectors of R, A is a diagonal matrix with eigen values on the diagonal. To check if a spectral decomposition went correctly, we multiply the three matrices.

```python
R_check = np.dot(np.dot(T, A), T.T)
R_check
```

Third, we calculate the percentage of variability that each of the principal components explains.

```python
total_sample_var = np.trace(R)
explained_variance = [(i / total_sample_var) * 100 for i in eig_vals]
pc_values = np.arange(len(R)) + 1
for i in range(len(R)):
    print(f'Principal component {pc_values[i]} explains {explained_variance[i]} of variability.')
```

Based on the percentage of explained variability, we assume that we may have to take 1 or 2 first components as their sum is equal 85.5%. Although we want to take a look at other methods for discarding components.

```python
plt.plot(pc_values, eig_vals, 'o-', linewidth=2, color='blue')
plt.axhline(1, color='r', label="Kaiser's criterion")
plt.axhline(0.7, color='r', label="Jollife's criterion", linestyle='dashed')
plt.title('Scree plot')
plt.xlabel('Principal component')
plt.ylabel('Eigen value')
plt.legend()
plt.show()
```

From the plot above we derive the following conclusions:
- Cattell's scree graph suggests taking 1 principal component as the steepness of the slope decreases significantly from the 2nd to the 3d component;
- Kaiser's criterion (computed from R) suggests taking only the 1st component as its value is bigger than 1;
- Jollife's criterion (computed from R) suggests taking 2 PCs as their eigenvalues are bigger than 0.7.

Unfortunately, here we cannot use the Dimensionality test to check the number of components as our data is standardized. As two tests allow us to use 2 principal components, we would like to continue with them for educational purposes.

## 1.2. Interpret the principal components

```python
pc_weights = np.stack((T[:, 0], T[:, 1]))
print(pd.DataFrame(pc_weights, columns=df_continuous.columns, index=['PC 1', 'PC 2']))
```

The first two components have the following form:
$$ \tilde{Y_1} = - 0.398182{X_1}^{*} - 0.440375{X_2}^{*}  - 0.454209{X_3}^{*} - 0.349186{X_4}^{*} - 0.310694{X_5}^{*} - 0.471968{X_6}^{*} $$
$$ \tilde{Y_2} = - 0.377136{X_1}^{*} - 0.218658{X_2}^{*}  - 0.262367{X_3}^{*} + 0.431004{X_4}^{*} + 0.744435{X_5}^{*} - 0.034243{X_6}^{*} $$

From $\tilde{Y_1}$ we see that all Xs contribute with negative weights to the first principal component. Such variables as "Trip Total", "Fare" and "Trip Miles" have the biggest magnitude and are related to the distance of a ride and its cost for a customer.
From $\tilde{Y_2}$ we derive that the most influential variables for the second principal component are "Tips" and "Extras" that contribute with a positive sign. These variables generally represent some additional services taken during the ride (for "Extras" it can be baby passenger seat or toll road) and customer's satisfaction (for "Tips"). Also, these two variables have different relative direction with other Xs
which makes us think that high values of them are associated with short and cheap rides.

To calculate the original observations represented in the new coordinate system of the first two principal components, we multiply:
$$ Y = XT $$

```python
Y1 = X @ T[:, 0]
Y2 = X @ T[:, 1]
Y = np.column_stack((Y1, Y2))

plt.scatter(Y1 * (-1), Y2, color='blue')
plt.title('PCA with explained variance of 85.5%')
plt.xlabel('1st principal component')
plt.ylabel('2nd principal component')
plt.show()
```

By changing the sign of the first principal component, we get that all variables contribute positively. On the scatter plot, it means that the rides on the right-hand side are larger both cost-wise and distance-wise. And the rides on the left-hand side are shorter and cheaper. At the same time, rides that are located higher are associated with more extra services and lower - with less spending on extra services. As the cumulative explained variance is higher than 85%, our PCA with two principal components seems to effectively summarize the data.

```python
df_pc_weights = pd.DataFrame(pc_weights.T, columns=['pc1 weights', 'pc2 weights'], index=pc_values)
df_pc_weights.plot.bar()
plt.axhline(0, color='black', linewidth=0.5)
plt.title('PCA weights')
plt.show()
```

- On the bar plot, we see that both PC1 and PC2 contribute negatively to $X_1$ ("Trip Seconds"), and their magnitudes are close. This suggests that increasing the duration of the ride decreases the scores for both principal components. In practical terms, longer rides are negatively associated with both trip characteristics and service-level features. This can indicate that longer rides tend to have fewer extras or tips.
- We can witness the same pattern for the PC1 for all Xs: the first principal component contributes negatively to all of them. As any variable increases, the PC1 score decreases. We suggest that PC1 represents an overall pattern where these variables jointly describe trip "size".
- PC2 has a positive contribution to $X_4$ ("Tips") and $X_5$ ("Extras"), and the last one is a dominating feature for the second principal component. Unlike PC1, which distinguishes rides based on their "size", PC2 distinguishes rides based on additional characteristics. Thus, higher PC2 scores represent rides with more extras and tips, and lower PC2 scores represent rides with fewer or no extras and tips.

```python
cross_correlations_Y1 = np.corrcoef(Y1, X.T)
cross_correlations_Y2 = np.corrcoef(Y2, X.T)
cross_corr = pd.DataFrame([cross_correlations_Y1[0][1:], cross_correlations_Y2[0][1:]], columns=df_continuous.columns,
                          index=['Y1', 'Y2'])
cross_corr
```
```python
sns.heatmap(cross_corr.T, annot=True, center=0, cmap='coolwarm')
plt.show()
```

Cross-correlation and heatmap associated with it confirm our conclusions. We witness high negative correlations (less than -0.8) between X and the first component for four variables associated with ride characteristics, and also a negative but slightly less strong correlation for two other variables - "Tips" and "Extras". At the same time, they have a positive correlation with the second component.

## 1.3. Study the stability of PCA

In order to analyze the contribution of each observation to the final model, we will design the stability test function. This test includes the following steps:

1) Exclude one row from the original dataset;
2) Recalculate PCA;
3) Project the removed row onto the principal components of the X with the excluded row;
4) Rebuild the configuration;
5) Compare the results calculating the Euclidean distance between the original PCA configuration and the transformed one.

```python
def pca_stability(X, Y):
    n, p = X.shape
    euclidean_dist = np.zeros((n, n))

    for i in range(X.shape[0]):
        exlud_x = X[i, :]
        X_excl = np.delete(X, i, axis=0)
        R = np.corrcoef(X_excl, rowvar=False)
        eig_vals, eig_vecs = eig(R)
        T = eig_vecs[:, :2]
        Y_excl = X_excl @ T
        y = exlud_x @ T
        Y_transformed = np.insert(Y_excl, i, y, axis=0)
        euclidean_dist[i, :] = np.linalg.norm(Y - Y_transformed, axis=1)
    return euclidean_dist

v = pca_stability(X, Y)
n = v.shape[0]
stats = np.zeros((n, 6))

for i in range(n):
    stats[i, 0] = np.min(v[i, :])
    stats[i, 1] = np.max(v[i, :])
    stats[i, 2] = np.quantile(v[i, :], 0.50)
    stats[i, 3] = np.quantile(v[i, :], 0.75)
    stats[i, 4] = np.quantile(v[i, :], 0.90)
    stats[i, 5] = np.quantile(v[i, :], 0.95)

rowsummary = (np.mean(stats, axis=0), np.std(stats, axis=0), np.median(stats, axis=0),
              median_abs_deviation(stats, axis=0))
stats_summary = pd.DataFrame(rowsummary, index=['mean', 'median', 'std', 'mad'],
                            columns=['min', 'max', 'median', '75th p', '90th p', '95th p'])
stats_summary
```

From the table that summarizes Euclidian distances for all 800 observations, we notice that the median values are quite small (the mean of the median value is equal 0.0009). On the other hand, the median value for a maximum (0.042) suggests that there are observations that may influence the stability of PCA more significantly. Let's do the boxplots for the first 20 values.

```python
plt.boxplot(v[:, :20])
plt.ylim(-0.02, 0.2)
plt.title('Euclidian distance of each unit among configurations')
plt.show()
```

The plots above prove that the median of all distances for each observation lies close to 0, and data is heavily skewed because none of the variables can take negative values. At the same time, there are certain outliers, and its exclusion may affect PCA more than we expected. Among them are observations 12, 13, 15, 16, 17, and others. As we stated in the first part of this chapter, the decision to keep all the observations was based on the fact that we would potentially lose at least 20% of the information.

As of now, we still consider these outliers important. Recalling the fact that the variables "Extras" and "Tips" had the highest number of outliers, we assume that their exclusion may significantly decrease the explained variance of PC2 to where both variables contribute with a positive sign.

```python
def stability_regions(Y, num_rows=None):
    radius = stats[:, 4] * 50
    theta = np.linspace(0, 2 * np.pi, 100)

    if num_rows is None:
        num_rows = len(Y)
    else:
        num_rows = min(num_rows, len(Y))

    for i in range(num_rows):
        if radius[i] > 0:
            plt.ylim(-3.5, 3.5)
            plt.scatter(Y[i, 0], Y[i, 1], c='blue', zorder=3, s=5)
            circle_x = Y[i, 0] + np.cos(theta) * radius[i]
            circle_y = Y[i, 1] + np.sin(theta) * radius[i]
            plt.plot(circle_x, circle_y, color='black', linewidth=1)
            plt.text(Y[i, 0] + 0.07, Y[i, 1] + 0.07, f"{i + 1}", fontsize=8, color='black')
            plt.title("PCA Representation with 90% stability regions")
            plt.xlabel("1st principal component")
            plt.ylabel("2nd principal components")
        else:
            pass


stability_regions(Y, num_rows=20)
```

Even on a small number of observations, we notice that stability regions differ significantly for any particular case. Thus, rides with small circles indicate that their removal will not affect the result of PCA dramatically. However, large regions around the observations point to influential rides that may affect PCA results. As we have seen on the boxplot graph: observations 12, 15, and 16 have the biggest 90% stability regions. Although observation number 12 has the furthest outlier, observation 16 has a bigger skewness in general that is represented by the biggest stability region.
