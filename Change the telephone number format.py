# Change the telephone number format

# Define a schema for the UDF output
schema = StructType([
    StructField("phone_numbers", ArrayType(StringType()), True),
    StructField("extension", StringType(), True)
])

def process_phone_number(number):
    
    if number is None or number =='0':
        return ([], None)

    # Remove all kinds of whitespaces
    number = re.sub(r"\s+", "", number)

    # Handle numbers with two hypans
    if (number.count('-') == 2) and not(number.count('/')):
        number = number[::-1].replace('-', '/', 1)[::-1]
        #print(number)

    # Handle numbers such as 3456789-2
    sub_check = number.split('-')
    if len(sub_check[0])>=7:
        number = number.replace("-","/")
        #print(sub_check)

    # Normalize the number
    normalized_number = number.replace(" ", "").replace("-", "").replace("+940","0").replace(",","/").replace("+94", "0")
    parts = normalized_number.split("ext")
    main_part = parts[0]
    extension = parts[1].strip() if len(parts) > 1 else None

    # Split the main part based on '/'
    parts = main_part.split("/")
    processed_numbers = []

    # Process each part
    for i, part in enumerate(parts):
        if i == 0:
            processed_numbers.append(part)
        else:
            # Handle continuation logic
            prev_part = processed_numbers[-1]
            if len(part) in [1, 2, 3, 4] and part.isdigit():
                # Construct continuation based on the length of the part
                continuation = prev_part[:-len(part)] + part
                processed_numbers.append(continuation)
            else:
                processed_numbers.append(part)

    return (processed_numbers, extension)


# Register UDF
process_phone_udf = udf(process_phone_number, schema)

# Apply transformations
df_brz = df_brz.withColumn("processed", process_phone_udf(col("loc_telephoneno")))