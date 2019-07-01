/****** Script for SelectTopNRows command from SSMS  ******/
SELECT conc.concept_code, count(*)
  FROM [OMOP].[dbo].[measurement] meas
  join [OMOP].[dbo].[concept] conc on meas.measurement_concept_id = conc.concept_id
  where conc.concept_code in ('74856-6',
'49573-9',
'48558-1',
'53798-5',
'34700-5',
'33630-5',
'30554-0',
'45176-5',
'45175-7',
'85380-4',
'9836-8',
'44533-8',
'75666-8',
'58900-2',
'56888-1',
'48346-1',
'48345-3',
'57975-5',
'22357-8',
'7918-6',
'44873-8',
'5223-3',
'31201-7',
'80387-4',
'73906-0',
'43009-0',
'40733-8',
'73905-2',
'43008-2',
'41290-8',
'85361-4',
'85368-9',
'32602-5',
'54086-4',
'43010-8',
'42600-7',
'49580-4',
'80695-0',
'80694-3',
'80692-7',
'80693-5',
'80691-9',
'79380-2',
'79379-4',
'74854-1',
'9837-6',
'44871-2',
'48023-6',
'78007-2',
'78009-8',
'88212-6',
'78010-6',
'78008-0',
'77368-9',
'74855-8',
'74853-3',
'21007-0',
'44531-2',
'47359-5',
'53923-9',
'88453-6',
'19110-6',
'35452-2',
'35564-4',
'35565-1',
'31072-2',
'21332-2',
'32842-7',
'32827-8',
'33508-3',
'12855-3',
'12857-9',
'12858-7',
'12870-2',
'12871-0',
'12872-8',
'12875-1',
'12876-9',
'12893-4',
'12894-2',
'12895-9',
'43013-2',
'14126-7',
'40439-2',
'16132-3',
'83325-1',
'80689-3',
'81122-4',
'80690-1',
'50790-5',
'57182-8',
'80688-5',
'87963-5',
'44532-0',
'9661-0',
'35441-5',
'9660-2',
'35440-7',
'43012-4',
'9662-8',
'40438-4',
'35446-4',
'9663-6',
'35449-8',
'12859-5',
'35450-6',
'44872-0',
'16978-9',
'43011-6',
'9664-4',
'21331-4',
'40437-6',
'9665-1',
'9821-0',
'53601-1',
'42339-2',
'18396-2',
'33660-2',
'16979-7',
'49718-0',
'35448-0',
'9666-9',
'35447-2',
'9667-7',
'35445-6',
'9668-5',
'35444-9',
'12856-1',
'35443-1',
'9669-3',
'35442-3',
'87962-7',
'73658-7',
'49483-1',
'44607-0',
'22356-0',
'43599-0',
'7917-8',
'14092-1',
'5220-9',
'29893-5',
'5221-7',
'68961-2',
'86233-4',
'85686-4',
'13499-9',
'16975-5',
'40732-0',
'16976-3',
'24012-7',
'5222-5',
'5017-9',
'23876-6',
'29539-4',
'59419-2',
'70241-5',
'24013-5',
'21333-0',
'10351-5',
'62469-2',
'41516-6',
'41514-1',
'29541-0',
'51780-5',
'48510-2',
'48552-4',
'21008-8',
'41513-3',
'41515-8',
'20447-9',
'48551-6',
'48511-0',
'25835-0',
'41145-4',
'33866-5',
'29327-4',
'34591-8',
'34592-6',
'16974-8',
'57974-8',
'42627-0',
'31430-2',
'28004-0',
'28052-9',
'16977-1',
'41497-9',
'41498-7',
'42917-5',
'41143-9',
'35438-1',
'41144-7',
'35437-3',
'35439-9',
'77369-7',
'32571-2',
'53379-4',
'89374-3',
'49905-3',
'49890-7',
'25836-8',
'5018-7',
'42768-2',
'69668-2',
'80203-3',
'43185-8',
'77685-6',
'73659-5',
'45212-8',
'57976-3',
'57977-1',
'57978-9',
'62456-9',
'10901-7',
'10902-5',
'11078-3',
'11079-1',
'11080-9',
'11081-7',
'11082-5',
'13920-4',
'21334-8',
'21335-5',
'21336-3',
'21337-1',
'21338-9',
'21339-7',
'21340-5',
'22358-6',
'7919-4',
'5225-8',
'5224-1',
'30361-0',
'81641-3',
'31073-0',
'51786-2',
'33807-9',
'33806-1',
'86548-5',
'69354-9',
'81652-0',
'69353-1',
'47029-4',
'86549-3',
'86547-7',
'80695-0',
'80694-3',
'6429-5',
'6430-3',
'6431-1',
'49573-9',
'48558-1',
'45182-3',
'83326-9',
'83327-7',
'53798-5',
'34700-5',
'30554-0',
'45176-5',
'88544-2',
'45175-7',
'49659-6',
'49664-6',
'33630-5',
'49656-2',
'49661-2',
'88543-4',
'88542-6',
'61199-6',
'21009-6',
'85037-0',
'89365-1',
'75622-1',
'49965-7',
'51866-2',
'30245-5',
'10682-3',
'50624-6',
'79155-8',
'83101-6',
'53825-6',
'38998-1',
'59052-1',
'25841-8',
'34699-9',
'25842-6',
'81246-1',
'48559-9',
'72560-6',
'72559-8',
'49657-0',
'49662-0',
'49658-8',
'49663-8',
'73695-9',
'49660-4',
'49665-3'
)
group by concept_code
;
