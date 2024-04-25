import psycopg2

# creates the connection (each time we want to access the database, must call this)
def createConnection():
    connection = psycopg2.connect(host="localhost", database="project", user="christoph", password="project")
    return connection

# method for inserting data
def insertData():
    conn = createConnection()
    cur = conn.cursor()

# printing all of the tables
    print("""
Select a table to insert a record to:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competiton
""")

    # ask for which table to insert into
    table = input("Pick a table ")

    # basically a big if statement
    # asks seperates questions tailored for each table if selected
    match table:
        case '1':
            # asks for columns of location
            city = input("Input the city ")
            state = input("Input the state ")
            street = input("Input street ")
            country = input("Input country ")
            # this line executes the sql command through psycopg2 - formatting is important according to documentation
            cur.execute("INSERT INTO Location (City, State, Street, Country) VALUES (%s, %s, %s, %s)", (city, state, street, country))

            # all of these cases follow this format of asking questions for each table (besides auto generated IDs)
            # so I don't think its neccessary to comment each one
        case '2':
            iname = input("Input the industry name ")
            cur.execute("INSERT INTO Industry (IName) VALUES (%s)", (iname,))
        
        case '3':
            sname = input("Input the sector name ")
            industryId = input("Input the industry ID ")
            cur.execute("INSERT INTO Sector (SName, IndustryID) VALUES (%s, %s)", (sname, industryId))
        
        case '4':
            hname = input("Input the headquarters name ")
            city = input("Input the city ")
            state = input("Input the state ")
            street = input("Input the street ")
            country = input("Input the country ")
            cur.execute("INSERT INTO Headquarters (HName, City, State, Street, Country) VALUES (%s, %s, %s, %s, %s)", (hname, city, state, street, country))

        
        case '5':
            title = input("Input the job title ")
            description = input("Input the job description ")
            salary = input("Input the salary ")
            city = input("Input the city ")
            state = input("Input the state ")
            street = input("Input the street ")
            country = input("Input the country ")
            sectorId = input("Input the sectorID ")
            headquartersId = input("Input the headquartersID ")
            cur.execute("INSERT INTO Job (Title, Description, Salary, City, State, Street, Country, SectorID, HeadquartersID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (title, description, salary, city, state, street, country, sectorId, headquartersId))

        
        case '6':
            cname = input("Input the company name ")
            size = input("Input the size of the company ")
            ownership = input("Input the ownership type (either public, private, orunknown) ")
            headquartersId = input("Input the headquartersID ")
            industryId = input("Input the industryID ")
            sectorId = input("Input the sectorID ")
            cur.execute("INSERT INTO Company (CName, Size, Ownership, HeadquartersID, IndustryID, SectorID) VALUES (%s, %s, %s, %s, %s, %s)", (cname, size, ownership, headquartersId, industryId, sectorId))

        
        case '7':
            competitorId = input("Input the competitorID (same as company ID) ")
            cur.execute("INSERT INTO Competitor (CompetitorID) VALUES (%s)", (competitorId,))
        # this case will be hit if anything besides 1-7 is entered from the user
        case _:
            # they did not print a valid option so I exit the application
            print("You did not pick a valid option... Exiting.")
            return

    # must commit the changes into the database
    conn.commit()

    # exits the cursor and connection safely
    cur.close()
    conn.close()
    
    # prints a success message so the user knows that their data was stored
    print("Your input has been saved in the database.")


# method for deleting data
def deleteData():
    # establishes the connection
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to delete a record from:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")
    
    # asks for which table
    table = input("Pick a table ")

    # asks specific questions dependent on table
    match table:
        case '1':
            # since the location has a primary key consisting of all these columns, need to ask for all of these
            city = input("Input the city ")
            state = input("Input the state ")
            street = input("Input the street ")
            country = input("Input the country ")
            # delete if all columns match
            cur.execute("DELETE FROM Location WHERE City = %s AND State = %s AND Street = %s AND Country = %s", (city, state, street, country))
        case '2':
            # otherwise (as is for most)
            # just need the unique ID and can remove off just that
            industryId = input("Input the industryID ")
            cur.execute("DELETE FROM Industry WHERE IndustryID = %s", (industryId,))

        
        case '3':
            sectorId = input("Input the sectorID ")
            cur.execute("DELETE FROM Sector WHERE SectorID = %s", (sectorId,))
        
        case '4':
            headquartersId = input("Input the headquartersID ")
            cur.execute("DELETE FROM Headquarters WHERE HeadquartersID = %s", (headquartersId,))
        
        case '5':
            jobId = input("Input the jobID ")
            cur.execute("DELETE FROM Job WHERE JobID = %s", (jobId,))
        
        case '6':
            companyId = input("Input the companyID ")
            cur.execute("DELETE FROM Company WHERE CompanyID = %s", (companyId,))

        
        case '7':
            competitorId = input("Input the competitorID ")
            cur.execute("DELETE FROM Competitor WHERE CompetitorID = %s", (competitorId,))
            
        case _:
            # if they did not pick a valid table
            print("You did not pick a valid option... Exiting.")
            return

    # commits and closes the connection
    conn.commit()
    cur.close()
    conn.close()

    # success statement
    print("Your data to be deleted has been removed from the database.")

# method for updating data
def updateData():
    # establishes the connection
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to update of a record in:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table = input("Pick a table ")

    # for each specific table
    # have to ask to find the current record they want to update
    # and also the data they want to change it to
    match table:
        case '1':
            # asks city, statae, street etc to find the record
            city = input("Input the current city ")
            state = input("Input the current state ")
            street = input("Input the current street ")
            country = input("Input the current country ")
            # asking for the new columns to replace them to (have to re enter everything even if they wanted to change one thing)
            # makes things on my end much simpler
            newCity = input("Input the new city ")
            newState = input("Input the new state ")
            newStreet = input("Input the new street ")
            newCountry = input("Input the new country ")
            # runs the sql command
            cur.execute("""UPDATE Location SET City = %s, State = %s, Street = %s, Country = %s WHERE City = %s AND State = %s AND Street = %s AND Country = %s""", (newCity, newState, newStreet, newCountry, city, state, street, country))

        # same idea is applied for the rest of the columns
        # but easier since can just reference ID to find record
        case '2':
            industryId = input("Input the industryID ")
            newName = input("Input the new name for the industry ")
            cur.execute("UPDATE Industry SET IName = %s WHERE IndustryID = %s", (newName, industryId))
        case '3':
            sectorId = input("Input the sectorID ")
            newName = input("Input the new name ")
            nenIndustryId = input("Input the new industryID ")
            cur.execute("UPDATE Sector SET SName = %s, IndustryID = %s WHERE SectorID = %s", (newName, newIndustryId, sectorId))
        case '4':
            headquartersId = input("Input the headquartersID ")
            newName = input("Input the new name for the headquarters ")
            newCity = input("Input the new city ")
            newState = input("Input the new state ")
            newStreet = input("Input the new street ")
            newCountry = input("Input the new country ")
            cur.execute("UPDATE Headquarters SET HName = %s, City = %s, State = %s, Street = %s, Country = %s WHERE HeadquartersID = %s", (newName, newCity, newState, newStreet, newCountry, headquartersId))
        case '5':
            jobId = input("Input the jobID ")
            newTitle = input("Input the new job title ")
            newDescription = input("Input the new job description ")
            newSalary = input("Input the new salary ")
            cur.execute("UPDATE Job SET Title = %s, Description = %s, Salary = %s WHERE JobID = %s", (newTitle, newDescription, newSalary, jobId))
        case '6':
            companyId = input("Input the company ID ")
            newName = input("Input the new company name ")
            newSize = input("Input the new size of the company ")
            newOwnership = input("Input the new ownership type (either public, private, or unkown) ")
            cur.execute("UPDATE Company SET CName = %s, Size = %s, Ownership = %s WHERE CompanyID = %s", (newName, newSize, newOwnership, companyId))
        case '7':
            competitorId = input("Input the competitorID ")
            newCompanyId = input("Input the new company ID ")
            cur.execute("UPDATE Competitor SET CompetitorID = %s WHERE CompetitorID = %s", (newCompanyId, competitorId))
        # if no option was picked
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    # commit and close
    conn.commit()
    cur.close()
    conn.close()

    print("Your modifications have been saved into the database.")

# method for searching for records
def searchData():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to search a record from:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table = input("Pick a table ")
    # set the table they want by the number they picked
    match table:
        case '1':
            table = 'Location'
        case '2':
            table = 'Industry'
        case '3':
            table = 'Sector'
        case '4':
            table = 'Headquarters'
        case '5':
            table = 'Job'
        case '6':
            table = 'Company'
        case '7':
            table = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    # ask for the column name they want to search for
    column = input("Input the column name ")
    # ask if they want greater than, equal to, or less than
    operator = input("Input the operator (either <, >, or =) ")
    # input the value of the column value to query based off
    value = input("Input the value to search for ")

    # executes the simple search command using where
    cur.execute(f"SELECT * FROM {table} WHERE {column} {operator} %s", (value,))

    # this gets all of the output from the query if there is any
    r = cur.fetchall()

    # if this exists and is not null (something actually came up)
    if r:
        # for each record found
        for i in r:
            # print it out
            print(i)
    else:
        # otherwise the return was empty and there were no results found
        print("No results were found.")

    # don't have to commit since nothing was planned to be changed in the database
    # however still have to close the connection
    cur.close()
    conn.close()

# method for aggregate function queries
def aggregateFunctions():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to perform aggregate functions on:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table = input("Pick a table ")

    # similar to search method, assign the value of the table from the number they picked
    match table:
        case '1':
            table = 'Location'
        case '2':
            table = 'Industry'
        case '3':
            table = 'Sector'
        case '4':
            table = 'Headquarters'
        case '5':
            table = 'Job'
        case '6':
            table = 'Company'
        case '7':
            table = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    print("""
Pick the aggregate function you want to use:

1. SUM
2. AVG
3. MAX
4. MIN
5. COUNT
""")

    # also asks which aggregation function they want to use
    function = input("Pick an aggregate function ")

    # finds the function name they want to use
    match function:
        case '1':
            functionName = 'SUM'
        case '2':
            functionName = 'AVG'
        case '3':
            functionName = 'MAX'
        case '4':
            functionName = 'MIN'
        case '5':
            functionName = 'COUNT'
        case _:
            # exits if no correct number was picked (can't do query without this)
            print("You did not pick a valid option... Exiting.")
            return

    # asks for the column name
    column = input(f"Input the column name ")

    # executes the sql command with the aggregated function
    cur.execute(f"SELECT {functionName}({column}) FROM {table}")

    # gets just the first result (only one should be returned anyways)
    r = cur.fetchone()

    # if it exists, print it, otherwise say no results where found (if the column they picked was incorrect/had no records)
    if r:
        print(f"The result is {r[0]}")
    else:
        print("No result found.")

    cur.close()
    conn.close()

# method for sorting
def sort():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to sort records from:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table = input("Pick a table ")

    match table:
        case '1':
            table = 'Location'
        case '2':
            table = 'Industry'
        case '3':
            table = 'Sector'
        case '4':
            table = 'Headquarters'
        case '5':
            table = 'Job'
        case '6':
            table = 'Company'
        case '7':
            table = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    # asks for the column to search by
    column = input("Input the column name to sort by ")
    # asks if they want to be sorted from ascending or descending
    order = input("Input sort order (ASC or DESC) ").upper()

    # return if they did write out one of the ASC or DESC correctly
    if order not in ['ASC', 'DESC']:
        print("You did not pick a valid option... Exiting.")
        return

    # executes the query
    cur.execute(f"SELECT * FROM {table} ORDER BY {column} {order}")

    # gets all of the records which should also be in the correct order
    r = cur.fetchall()

    # if something was found
    if r:
        # for each one
        for i in r:
            # print it out
            print(i)
    else:
        # otherwise say no data was found
        print("No data was found.")

    # closing the connection
    cur.close()
    conn.close()

# method for joining
def join():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select the first table for the join:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table1 = input("Pick the first table ")
    # finds the first table
    match table1:
        case '1':
            table1 = 'Location'
        case '2':
            table1 = 'Industry'
        case '3':
            table1 = 'Sector'
        case '4':
            table1 = 'Headquarters'
        case '5':
            table1 = 'Job'
        case '6':
            table1 = 'Company'
        case '7':
            table1 = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    print("""
Select the second table for the join:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    # finds the second table
    table2 = input("Pick the second table ")

    match table2:
        case '1':
            table2 = 'Location'
        case '2':
            table2 = 'Industry'
        case '3':
            table2 = 'Sector'
        case '4':
            table2 = 'Headquarters'
        case '5':
            table2 = 'Job'
        case '6':
            table2 = 'Company'
        case '7':
            table2 = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    print("""
Choose the type of join:

1. INNER JOIN
2. LEFT JOIN
3. RIGHT JOIN
4. FULL OUTER JOIN
""")
    
    # get which type of join they want and assigns it to joinType
    joinType = input("Pick a join type ")

    match joinType:
        case '1':
            joinType = 'INNER JOIN'
        case '2':
            joinType = 'LEFT JOIN'
        case '3':
            joinType = 'RIGHT JOIN'
        case '4':
            joinType = 'FULL OUTER JOIN'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    # gets the attributes aka columns they want to pull from each
    attribute = input(f"Input the join attribute from the first table ")
    attribute2 = input(f"Input the join attribute from the second table")

    # executes the actual command
    cur.execute(f"SELECT * FROM {table1} {joinType} {table2} ON {table1}.{attribute} = {table2}.{attribute2}")

    # gets all of the records that are returned
    r = cur.fetchall()

    # prints it out (same printing logic as before)
    if r:
        for i in r:
            print(i)
    else:
        print("No data was found.")

    # closes the connection
    cur.close()
    conn.close()

# group method
def group():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select a table to perform grouping on:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table = input("Pick a table: ")
    match table:
        case '1':
            table = 'Location'
        case '2':
            table = 'Industry'
        case '3':
            table = 'Sector'
        case '4':
            table = 'Headquarters'
        case '5':
            table = 'Job'
        case '6':
            table = 'Company'
        case '7':
            table = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    # finds the column to group by
    column = input("Input the column to group by ")

    # finds the disctinct value in this column from the table they requested
    cur.execute(f"SELECT DISTINCT {column} FROM {table} ORDER BY {column}")

    # finds all of the records, not just one
    r = cur.fetchall()
    
    # returns records
    if r:
        print(f"Unique values in {column} of {table}:")
        for i in r:
            print(i[0])
    else:
        print("No data was found.")

    # closes connection
    cur.close()
    conn.close()

# subquering methods
def subquery():
    conn = createConnection()
    cur = conn.cursor()

    print("""
Select the main table to work with:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table1 = input("Pick the first table ")
    # asks for the first table in the main query
    match table1:
        case '1':
            table1 = 'Location'
        case '2':
            table1 = 'Industry'
        case '3':
            table1 = 'Sector'
        case '4':
            table1 = 'Headquarters'
        case '5':
            table1 = 'Job'
        case '6':
            table1 = 'Company'
        case '7':
            table1 = 'Competitor'
        case _:
            print("You did not pick a valid option... Exiting.")
            return

    print("""
Select the subquery table:

1. Location
2. Industry
3. Sector
4. Headquarters
5. Job
6. Company
7. Competitor
""")

    table2 = input("Pick the second table ")
    # asks for the second table in the subquery
    match table2:
        case '1':
            table2 = 'Location'
        case '2':
            table2 = 'Industry'
        case '3':
            table2 = 'Sector'
        case '4':
            table2 = 'Headquarters'
        case '5':
            table2 = 'Job'
        case '6':
            table2 = 'Company'
        case '7':
            table2 = 'Competitor'
        case _:

            print("You did not pick a valid option... Exiting.")
            return

    # asks for the columns of choice from each table in the queries
    column1 = input(f"Input the column from {table1} ")
    column2 = input(f"Input the column from {table2} ")
    # asks for the condition where they must be equal (for simplicity sake)
    cond = input(f"Input the value for the {column2} in {table2} ")

    # executes this command
    cur.execute(f"SELECT {column1} FROM {table1} WHERE {column1} IN (SELECT {column2} FROM {table2} WHERE {column2} = %s)", (cond,))

    # gets all of the records
    r = cur.fetchall()

    # and prints them
    if r:
        print(f"Results from {table1} and {table2}:")
        for i in r:
            print(i)
    else:
        print("No results were found.")

    # closes the connection
    cur.close()
    conn.close()

# logic for transactions
def transaction():
    conn = createConnection()
    cur = conn.cursor()

    # does a while statement (each loop is a sql command), this way it can do as many as the user wants
    try:
        print("Transaction started. You can do multiple requests back to back.")

        # has a new option to leave and commit what they have
        while True:
            print("""
Select an operation to perform within the transaction:

1. Insert Data
2. Delete Data
3. Update Data
4. Search Data
5. Aggregate Functions
6. Sorting
7. Join
8. Grouping
9. Subqueries
11. Commit and Exit
""")

            choice = input("Pick an option ")
            # calls the method of which they want, and then that method takes care of it
            match choice:
                case '1':
                    insertData()
                case '2':
                    deleteData()
                case '3':
                    updateData()
                case '4':
                    searchData()
                case '5':
                    aggregateFunctions()
                case '6':
                    sort()
                case '7':
                    join()
                case '8':
                    group()
                case '9':
                    subquery()
                case '11':
                    # if they choose to commit, then we finally commit everything
                    conn.commit()
                    print("Everything was commited to the database. ")
                    break
                case _:
                    # if they don't do something valid, then nothing was commited and it is all lost
                    print("You did not pick a valid option... Exiting.")

    except Exception as e:
        # on an error, it rollback to the preiovus commit so that nothing was saved from this
        # the all or nothing of a transaction
        conn.rollback()
        print("Something went wrong, nothing was saved.")
    finally:
        # after all of this, then it closes the connection
        cur.close()
        conn.close()

# MAIN METHOD THAT GETS RUN AT START
def main():
    # ask which query they want to do
    print("""
Welcome to the Data Scientist Job Database

Select One:

1. Insert Data
2. Delete Data
3. Update Data
4. Search Data
5. Aggregate Functions
6. Sorting
7. Join
8. Grouping
9. Subqueries
10. Transactions
""")

    userInput = input("Pick an option ")

    # depending on the number they pick, it runs the method associated with it
    match userInput:
        case '1':
            insertData()
        case '2':
            deleteData()
        case '3':
            updateData()
        case '4':
            searchData()
        case '5':
            aggregateFunctions()
        case '6':
            sort()
        case '7':
            join()
        case '8':
            group()
        case '9':
            subquery()
        case '10':
            transaction()
        case _:
            # if they pick something wrong it aborts
            print("You did not pick an option... Exiting")

if __name__ == "__main__":
    main()
