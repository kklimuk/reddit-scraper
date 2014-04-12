import numpy as np
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.lda import LDA
from sklearn.qda import QDA

names = ["Nearest Neighbors", "Linear SVM", "RBF SVM", "Decision Tree",
         "Random Forest", "AdaBoost", "Naive Bayes", "LDA", "QDA"]

classifiers = [
    [KNeighborsClassifier(2), KNeighborsClassifier(3), KNeighborsClassifier(4), KNeighborsClassifier(5)],
    [SVC(kernel="linear", C=0.01), SVC(kernel="linear", C=0.025), SVC(kernel="linear", C=0.05), SVC(kernel="linear", C=0.075)],
    [SVC(gamma=1.5, C=1), SVC(gamma=2, C=1), SVC(gamma=2.5, C=1)],
    [DecisionTreeClassifier(max_depth=3), DecisionTreeClassifier(max_depth=4), DecisionTreeClassifier(max_depth=5)],
    [RandomForestClassifier(max_depth=3, n_estimators=10, max_features=1), RandomForestClassifier(max_depth=4, n_estimators=10, max_features=1), RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1)],
    [AdaBoostClassifier()],
    [GaussianNB()],
    [LDA()],
    [QDA()]]

    # iterate over classifiers
for name, clf in zip(names, classifiers):
	for c in clf:
	    ax = pl.subplot(len(datasets), len(classifiers) + 1, i)
	    clf.fit(X_train, y_train)
	    score = clf.score(X_test, y_test)