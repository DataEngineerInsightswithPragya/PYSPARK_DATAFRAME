def masking(email_id):
    splitted_val = email_id.split("@")
    print("splitted_val ",splitted_val)
    initial = splitted_val[0]
    domain = splitted_val[1]
    print("initial ",initial)
    print("domain ",domain)


    length = len(initial)

    masked_mail_id = initial[0] + (length - 2) * '*' + initial[-1] + '@' + domain

    print("masked_mail_id : ",masked_mail_id)



if __name__ == "__main__":
    email_id = input("Enter the mail_id ")
    masking(email_id)
