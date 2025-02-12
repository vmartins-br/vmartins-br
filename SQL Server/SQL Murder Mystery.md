O código do desafio está aqui porquê é feito através do site:

https://mystery.knightlab.com/

SQL Murder Mystery

Primeiro vamos à cena do crime ver o que aconteceu, a data foi 20180115 ou 15 de Janeiro de 2018:

SELECT * FROM crime_scene_report 
WHERE city ="SQL City" 
AND date ="20180115";

20180115 - murder - 	Security footage shows that there were 2 witnesses. The first witness lives at the last house on "Northwestern Dr". 
			The second witness, named Annabel, lives somewhere on "Franklin Ave".	
SQL City

Agora vamos ver as casas registradas na rua "Northwestern Dr" e pegar o endereço da primeira testemunha:

SELECT * FROM person 
WHERE address_street_name = "Northwestern Dr" 
ORDER BY address_number DESC;


id	name		license_id	address_number	address_street_name	ssn
14887	Morty Schapiro	118009		4919		Northwestern Dr		111564949

A segunda testemunha chama Annabel e mora em algum lugar em "Franklin Ave":

SELECT * FROM person 
WHERE address_street_name = "Franklin Ave" 
AND name LIKE "%Annabel%";

Só há uma:

id	name		license_id	address_number	address_street_name	ssn
16371	Annabel Miller	490173		103		Franklin Ave		318771143

Agora que já sabemos as testemunhas, vamos ler seus relatos:

SELECT * FROM interview 
WHERE person_id = "14887";

person_id	transcript
14887		I heard a gunshot and then saw a man run out. He had a "Get Fit Now Gym" bag. 
		The membership number on the bag started with "48Z". Only gold members have those bags. 
		The man got into a car with a plate that included "H42W".

SELECT * FROM interview 
WHERE person_id = "16371";

person_id	transcript
16371		I saw the murder happen, and I recognized the killer from my gym when 
		I was working out last week on January the 9th.

Agora já sabemos que o suspeito (homem) é membro "48Z" é membro gold da academia, possui um carro com a placa "H42W" 
E estava presente dia 9 de Janeiro de 2018.

Vamos aos clientes registrados com código "48Z"

SELECT * FROM get_fit_now_member 
WHERE id LIKE "%48Z%";

id	person_id	name		membership_start_date	membership_status
48Z7A	28819		Joe Germuska	20160305		gold
48Z55	67318		Jeremy Bowers	20160101		gold

Vamos pegar mais informações dos suspeitos:

PRIMEIRO SUSPEITO:

id	name		license_id	address_number	address_street_name	ssn
28819	Joe Germuska	173289		111		Fisk Rd			138909730


SEGUNDO SUSPEITO:


id	name		license_id	address_number	address_street_name	ssn
67318	Jeremy Bowers	423327		530		Washington Pl, Apt 3A	871539279


PELA LICENÇA DE MOTORISTA PODEMOS ACHAR O QUE TEM A PLACA DO CARRO IGUAL A DO ASSASSINO:

SELECT * FROM drivers_license 
WHERE id = "173289";

(NÃO RETORNOU NADA)

SELECT * FROM drivers_license 
WHERE id = "423327";		(Jeremy Bowers)

id	age	height	eye_color	hair_color	gender	plate_number	car_make	car_model
423327	30	70	brown		brown		male	0H42W2		Chevrolet	Spark LS

Já temos 2 indícios contra Jeremy Bowers, vamos ver se estava na academia dia 9 de Janeiro de 2018:

SELECT * FROM get_fit_now_check_in 
WHERE check_in_date = "20180109" 
AND membership_id LIKE "48Z55";

membership_id	check_in_date	check_in_time	check_out_time
48Z55		20180109	1530		1700

Agora vamos ver seu relato:

person_id	transcript
67318		I was hired by a woman with a lot of money. 
		I don't know her name but I know she's around 5'5" (65") or 5'7" (67"). 
		She has red hair and she drives a Tesla Model S. 
		I know that she attended the SQL Symphony Concert 3 times in December 2017.

Temos uma nova suspeita, vamos procura-la:

SELECT * FROM drivers_license 
WHERE hair_color LIKE "%red%" 
AND gender = "female" 
AND car_make LIKE "%Tesla%" 
ND car_model LIKE "%S%";


id	age	height	eye_color	hair_color	gender	plate_number	car_make	car_model
202298	68	66	green		red		female	500123		Tesla		Model S
291182	65	66	blue		red		female	08CM64		Tesla		Model S
918773	48	65	black		red		female	917UU3		Tesla		Model S

Temos 3 suspeitas, vamos descobrir os nomes;

SELECT * FROM person 
WHERE license_id = "202298";

id	name			license_id	address_number	address_street_name	ssn
99716	Miranda Priestly	202298		1883		Golden Ave		987756388

SELECT * FROM person 
WHERE license_id = "291182";

id	name			license_id	address_number	address_street_name	ssn
90700	Regina George		291182		332		Maple Ave		337169072

SELECT * FROM person 
WHERE license_id = "918773";

id	name		license_id	address_number	address_street_name		ssn
78881	Red Korb	918773		107		Camerata Dr			961388910

agora vamos ver qual das 3 estavam no SQL Symphony Concert 3 vezes em Dezembro de 2017:

A PRIMEIRA:

SELECT * FROM facebook_event_checkin 
WHERE person_id = "99716" 
AND event_name = "SQL Symphony Concert" 
AND date LIKE "%201712%";

person_id	event_id	event_name		date
99716		1143		SQL Symphony Concert	20171206
99716		1143		SQL Symphony Concert	20171212
99716		1143		SQL Symphony Concert	20171229

A SEGUNDA:

SELECT * FROM facebook_event_checkin 
WHERE person_id = "90700" AND event_name = "SQL Symphony Concert" 
AND date LIKE "%201712%";

(Não retornou nada)

A TERCEIRA:

SELECT * FROM facebook_event_checkin 
WHERE person_id = "78881" 
AND event_name = "SQL Symphony Concert" 
AND date LIKE "%201712%";

(Não retornou nada)

Provavelmente é a primeira, mas vamos pegar mais informações, vamos ver se ela recebe mais anualmente que as outras:

SELECT * FROM income 
WHERE ssn = "987756388";

ssn		annual_income
987756388	310000

SELECT * FROM income 
WHERE ssn = "337169072";

(Não retornou nada)

SELECT * FROM income 
WHERE ssn = "961388910";

ssn		annual_income
961388910	278000

Nossa suspeita de mandato de assassinato é Miranda Priestly e o assassino é Jeremy Bowers.

[Vamos checar a solução na página](https://mystery.knightlab.com/)

value
Congrats, you found the brains behind the murder! Everyone in SQL City hails you as the greatest SQL detective of all time. Time to break out the champagne!
              

