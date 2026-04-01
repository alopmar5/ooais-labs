with open("data/raw/observations.csv") as f:
	lines = f.readlines()
records = lines[1:]

temps = []
object_count = {}
invalid_count = 0

for line in records:
    parts = line.strip().split(",")
    timestamp = parts[0]
    obj = parts[1]
    temp = parts[2]
    object_count[obj] = object_count.get(obj, 0) + 1

    if temp == "INVALID":
        invalid_count += 1
        continue
    temps.append(float(temp))


avg_temp = sum(temps) / len(temps)

print("=== DATA SUMMARY ===")
print(f"Total records: {len(records)}")
print(f"Valid records: {len(temps)}")
print(f"Invalid records: {invalid_count}")
print(f"Average temperature: {avg_temp:.2f}")

print("\n=== OBJECT DISTRIBUTION ===")
for obj, count in object_count.items():
    print(f"{obj}: {count}")
