def min_swap(A,K):

    cnt =0
    l = len(A)
    for i in range (l):
        if A[i] <= K :
            cnt = cnt+1

    bad = 0
    for i in range (cnt):
        if A[i] > K:
            bad = bad + 1
    ans = bad

    i = 0
    j = cnt

    while ( j < l):
        if A[i] > K:
            bad = bad -1
        if A[j] > K:
            bad = bad + 1
        ans = min(ans,bad)
        i = i+1
        j = j+1

    print("minimum swap required is : ",ans)


A = [1, 12, 10, 3, 14, 10, 5]
K = 8

min_swap(A,K)