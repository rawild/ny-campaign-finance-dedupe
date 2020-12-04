def generate_word_list(word):
    word_list = []
    word_list.append(word)
    word_consonants = word
    for vowel in ['a','e','i','o','u']:
        word_consonants = word_consonants.replace(vowel,'')
    suffix  = word[-1] if word[-1] != word_consonants[-1] else ''
    for i in range(2,6):
        if i > 2:
            word_list.append(word[0:i])
            word_list.append(word[0:i]+word_consonants[-1])
            word_list.append(word[0:i]+word_consonants[-2:])
        word_list.append(word[0]+word_consonants[0:i])
    return word_list

def clean_donors():
    llc_regex = r"lim\w*\s*lia\w*\s*c\w*\s*"
    llp_regex = r"lim\w*\s*lia\w*\s*p\w*\s*"
    pac_regex = r"pol\w*\s*ac\w*\s*co\w*\s*"


if __name__ == '__main__':
   print("haha - I don't do anything")