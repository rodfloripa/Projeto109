---

<p align="justify"><h1>Projeto109: Treinamento Distribuído de Modelos de Machine Learning com Ray</h1></p>

<p align="justify">Este documento apresenta uma análise detalhada sobre as vantagens do uso do framework <b>Ray</b> para o treinamento distribuído e otimização de modelos de Machine Learning (como XGBoost e ensembles com balanceamento por SMOTE). Além disso, é apresentada a explicação completa de cada bloco de código utilizado para inicialização, compartilhamento de memória em tempo de execução, execução remota paralela e avaliação de métricas de desempenho.</p>

<p align="justify"><h3>1. As Vantagens do Treinamento Distribuído com Ray</h3></p>

<p align="justify">O treinamento de modelos estatísticos e redes neurais em larga escala frequentemente enfrenta gargalos de CPU, memória e escalabilidade quando executado de forma sequencial ou via threads padrão em Python (devido ao Global Interpreter Lock - GIL). O <b>Ray</b> supera essas limitações através das seguintes vantagens estruturais:</p>

* <p align="justify"><b>Paralelismo Assíncrono e Distribuído:</b> O Ray permite transformar funções e classes Python normais em tarefas remotas (<i>Tasks</i>) e atores (<i>Actors</i>), executando múltiplos treinamentos concorrentemente em todos os núcleos de CPU ou nós do cluster de forma assíncrona.</p>
* <p align="justify"><b>Object Store Compartilhado (Zero-Copy Memory):</b> Através do Plasma Object Store, o Ray armazena conjuntos de dados na memória compartilhada em formato de memória alinhada (Apache Arrow). Isso permite que múltiplos trabalhadores (<i>Workers</i>) acessem os dados de treino sem a necessidade de duplicar ou serializar/deserializar os dados repetidamente via <i>pickle</i>.</p>
* <p align="justify"><b>Escalabilidade Transparente:</b> O mesmo código construído localmente em um notebook pode ser dimensionado de 2 núcleos de CPU para centenas de nós em nuvem (AWS, GCP, Azure) sem a necessidade de reescrever o pipeline de treinamento.</p>
* <p align="justify"><b>Gestão Dinâmica de Tarefas e Resiliência:</b> O Ray gerencia o agendamento de tarefas e o balanceamento de carga dinamicamente. Se um nó ou processo falhar, o Ray reexecuta automaticamente as tarefas pendentes garantindo tolerância a falhas.</p>

<p align="justify"><h3>2. Explicação dos Blocos de Código Principais</h3></p>

<p align="justify">Abaixo detalhamos cada uma das etapas e funções que compõem o pipeline de treinamento do ensemble distribuído no Ray:</p>

<p align="justify"><b>1. Inicialização do Cluster Ray (<code>ray.init</code>)</b></p>

<p align="justify">O processo inicia-se encerrando qualquer instância pré-existente via <code>ray.shutdown()</code> e alocando um cluster local limpo com <code>ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count())</code>. Essa etapa mapeia os recursos de hardware disponíveis (CPUs, memória RAM e memória de objetos) e disponibiliza o motor de agendamento do Ray.</p>

<p align="justify"><b>2. Armazenamento Eficiente no Object Store (<code>ray.put</code>)</b></p>

<p align="justify">Antes de disparar as chamadas paralelas, os arrays <code>X_train</code> e <code>y_train</code> são registrados na memória compartilhada utilizando a função <code>ray.put()</code>:</p>

<p align="center">
<b>X_train_ref = ray.put(X_train)</b><br>
<b>y_train_ref = ray.put(y_train)</b>
</p>

<p align="justify">Ao invés de passar cópias massivas de dados para cada trabalhador, passamos apenas as referências (<i>ObjectRefs</i>). Cada trabalhador lê os dados diretamente do armazenamento de objetos com <code>ray.get()</code> de maneira ultrarrápida.</p>

<p align="justify"><b>3. Definição da Tarefa Remota (<code>@ray.remote</code>) e Pipeline Individual</b></p>

<p align="justify">O decorador <code>@ray.remote</code> transforma a função Python <code>treinar_xgb_ray(i)</code> em um processo assíncrono capaz de rodar isoladamente em um processo <i>Worker</i>. Dentro de cada worker:</p>

* <p align="justify">O dataset é resgatado via <code>ray.get(X_train_ref)</code>.</p>
* <p align="justify">A técnica <b>SMOTE</b> sintetiza amostras para balanceamento de classe específico para a semente <code>i</code>.</p>
* <p align="justify">O modelo <b>XGBClassifier</b> é treinado utilizando algoritmo de histograma (<code>tree_method='hist'</code>) configurado com <code>n_jobs=1</code> para permitir que o Ray gerencie o paralelismo entre os modelos no nível superior.</p>

<p align="justify"><b>4. Disparo Assíncrono e Coleta Progressiva (<code>ray.wait</code> / <code>tqdm</code>)</b></p>

<p align="justify">Os 10 modelos do ensemble são disparados simultaneamente usando compreensões de lista com a sintaxe <code>.remote()</code>: <code>futures_ray = [treinar_xgb_ray.remote(i) for i in range(N_MODELOS)]</code>. Para monitorar a conclusão sem bloquear a execução principal, utiliza-se o loop com <code>ray.wait()</code>:</p>

<p align="justify">O método <code>ray.wait(remaining, num_returns=1)</code> separa as tarefas finalizadas (<code>done</code>) das pendentes (<code>remaining</code>), permitindo atualizar a barra de progresso em tempo real e coletar os modelos treinados à medida que cada nó conclui o processamento.</p>

<p align="justify"><b>5. Avaliação Consolidada do Ensemble (<code>avaliar</code>)</b></p>

<p align="justify">A função de avaliação calcula a previsão combinada dos 10 modelos através de votação por média de probabilidades. Em seguida, extrai métricas robustas para problemas desbalanceados, incluindo <b>F1-Score</b>, <b>Precision-Recall AUC (PR AUC)</b>, <b>Precisão</b> e <b>Recall</b> para a classe minoritária.</p>

<p align="justify"><h3>3. Resultados e Métricas de Desempenho</h3></p>

<p align="justify">Na execução do pipeline distribuído, a alocação paralela reduziu o tempo total de treinamento de 10 modelos XGBoost com sobreamostragem por SMOTE para apenas <b>4.83 segundos</b>[cite: 4], alcançando excelente poder preditivo:</p>

* <p align="justify"><b>Tempo Total de Execução (Ray):</b> 4.83s[cite: 4]</p>
* <p align="justify"><b>F1-Score do Ensemble:</b> 0.9062[cite: 4]</p>
* <p align="justify"><b>PR AUC (Área sob a Curva Precision-Recall):</b> 0.9767[cite: 4]</p>
* <p align="justify"><b>Precisão (Classe 1):</b> 0.95[cite: 4]</p>
* <p align="justify"><b>Recall (Classe 1):</b> 0.87[cite: 4]</p>

<p align="justify"><h3>Conclusão</h3></p>

<p align="justify">A utilização do <b>Ray</b> para pipelines de Machine Learning elimina gargalos de processamento ao transformar tarefas pesadas e repetitivas em execuções distribuídas eficientes. O uso combinado de tarefas remotas assíncronas com o armazenamento de memória de cópia zero (<i>Object Store</i>) proporciona ganhos expressivos de velocidade, escalabilidade e produtividade em ambientes de Ciência de Dados.</p>

---
