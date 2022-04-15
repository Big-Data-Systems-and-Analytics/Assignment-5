import json
import requests


def en_to_esp(episodesumm,epner,eventsumm,eventner):
    epsumm_esp = requests.post('https://oxbebbtusa.execute-api.us-east-1.amazonaws.com/dev/abcd',json={"episode_narrative":episodesumm})
    epner_esp = requests.post('https://oxbebbtusa.execute-api.us-east-1.amazonaws.com/dev/abcd',json={"episode_narrative":epner})
    evsumm_esp = requests.post('https://oxbebbtusa.execute-api.us-east-1.amazonaws.com/dev/abcd',json={"episode_narrative":eventsumm})
    evner_esp = requests.post('https://oxbebbtusa.execute-api.us-east-1.amazonaws.com/dev/abcd',json={"episode_narrative":eventner})
    
    answer_epsumm_esp = json.loads(epsumm_esp.content.decode())
    answer_epner_esp = json.loads(epner_esp.content.decode())
    answer_evsumm_esp = json.loads(evsumm_esp.content.decode())
    answer_evner_esp = json.loads(evner_esp.content.decode())
    print(answer_epsumm_esp)
    return answer_epsumm_esp['answer'][0]['translation_text'], answer_epner_esp['answer'][0]['translation_text'],answer_evsumm_esp['answer'][0]['translation_text'],answer_evner_esp['answer'][0]['translation_text'],