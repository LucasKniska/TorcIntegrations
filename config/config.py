# Tells if the script should be run in test mode or production
production = True
motiveProduction = True

# Cookie to the fluke
production_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJmYTJkODc5Mi04ZjFjLTRmZDEtOGExYS04NGY2ZjZhYmU3NjgiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.FgsUCL81lnh6DJv6Ec4fuT5gyNtqKyeFgEx_Etz8CDo"
sandbox_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"

# Motive API Key
key = "9e90504a-82f0-4ed4-b54c-ce37f388f211" if motiveProduction else "ab7e71b6-e38e-469b-93ac-3b50b81aa8bd" # - This is the key for the sandbox account

headers = {
    "Content-Type": "application/json", 
    "Cookie": "JWT-Bearer=" + production_key if production else "JWT-Bearer=" + sandbox_key,
}

motive_headers = {
    "accept": "application/json", 
    "X-Api-Key": key
}