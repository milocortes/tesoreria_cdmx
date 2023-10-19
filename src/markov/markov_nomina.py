import numpy as np
import os 

from matplotlib import pylab as plt
import scipy.stats

import pandas as pd 

## Paquetes para estimación de modelos ocultos de markov
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
from tensorflow_probability import distributions as tfd

## API para S3
import boto3

## Carga datos de S3
s3 = boto3.client('s3') 
obj = s3.get_object(Bucket= os.environ["BUCKET"], Key=os.environ["S3_OBJ"]) 
initial_df = pd.read_csv(obj['Body'])


def hmm_tfp_batch(ts_data : np.array , num_states : int) -> (np.array, np.array, np.array):
    """
    Recibe un arreglo de numpy de la serie de tiempo y estima el Modelo Oculto de Markov
    dada la cantidad de estados ocultos definidos por el usuario.
    
    Argumentos de la Function
    ------------------
    - ts_data: arreglo de numpy de la serie de tiempo
    - num_states : número de estados ocultos

    ArgumentosKeyword 
    -----------------
    """
    # Definimos la distribución inicial para los estados ocultos.
    # Definimos 10 distribuciones iniciales para cada estado que siguen una distribución normal estándar.
    initial_logits = tf.Variable(tf.random.normal([10, 1, num_states]),
                                name='initial_logits')
    
    # Definimos la matriz de transición con distribución inicial uniforme en todos los estados ocultos.
    # Esto es, la probabilidad de transitar a cualquier estado es la misma. 
    transition_logits = tf.Variable(tf.random.normal([10, 1, num_states, num_states]),
                                    name='transition_logits')

    # Definimos las variables a entrenar las cuales representan las distribuciones normales (desconocidas) asociadas 
    # a cada sistema de los estados ocultos.
    training_loc =  tf.Variable(np.mean(ts_data) +tf.random.stateless_normal([num_states], seed=(42, 42)))
    training_scale = tf.Variable(np.std(ts_data) +tf.random.stateless_normal([num_states], seed=(42, 42)))

    # Suponemos que, para todos los estados, la media y desviación estándar son iguales a la media y desviación estándar
    # observada en toda la serie de tiempo.
    observation_distribution = tfp.distributions.Normal(loc= training_loc, scale=training_scale) 

    # Inicializamos la clase HiddenMarkovModel 
    trainable_hmm = tfd.HiddenMarkovModel(
        initial_distribution = tfd.Categorical(logits=initial_logits),
        transition_distribution = tfd.Categorical(transition_logits),
        observation_distribution=observation_distribution,
        num_steps=len(ts_data))

    # AJustamos la serie de tiempo al formato adecuado 
    training_data = tf.convert_to_tensor(ts_data.reshape((1, len(ts_data))).astype("float32"))
    
    # Definimos la log densidad del modelo incluido una distribución LogNormal a priori sobre las distribuciones 
    # de los sistemas de los estados ocultos. Se optimiza con Adam para calcular el maximum a posteriori (MAP)
    # que ajuste a la serie de tiempo.

    loss_curve = tfp.math.minimize(
        loss_fn=lambda: -tf.reduce_sum(trainable_hmm.log_prob(training_data), axis=-1),
        num_steps=500,
        optimizer=tf.optimizers.Adam(0.1))

    ## Corremos 10 modelos 
    final_losses = loss_curve[-1]
    print("Perdidas finales", final_losses)
    best_model_idx = tf.argmin(final_losses)
    print("Mejor pérdida {} (modelo {})".format(final_losses[best_model_idx],best_model_idx))

    print("Distribución inicial")
    print(trainable_hmm.initial_distribution.probs_parameter()[best_model_idx])

    print("Distribución de las transiciones")
    print(trainable_hmm.transition_distribution.probs_parameter()[best_model_idx])

    print("Distribución de las observaciones. Estrenada")

    print(f"Loc : {trainable_hmm.observation_distribution.loc.numpy()}")
    print(f"Scale : {trainable_hmm.observation_distribution.scale.numpy()}")

    # Nos quedamos con el modelo con la menor pérdida
    most_probable_states = trainable_hmm.posterior_mode(training_data)[best_model_idx][0]

    predicted_average = np.array([trainable_hmm.observation_distribution.loc.numpy()[state] for state in most_probable_states])
    predicted_std = np.array([trainable_hmm.observation_distribution.scale.numpy()[state] for state in most_probable_states])

    # Regresamos:
    # - Las medias estimadas para cada uno de los sistemas de los estados ocultos
    # - Las desviaciones estándar estimadas para cada uno de los sistemas de los estados ocultos
    # - La matriz de transición entre cada uno de los estados ocultos

    return predicted_average, predicted_std, trainable_hmm.transition_distribution.probs_parameter()[best_model_idx]


### Filtramos los datos para obtener únicamente la información del contribuyente 36239
ind_id = 5549
individuo = initial_df.query(f"V10==1 and V1=={ind_id}")[["V6_new", "V8"]]

### Entrenamos el modelo oculto de markov para la serie de tiempo de los pagos del contribuyente

### Definimos 2 estados ocultos
# Aplicamos una transformación logarítmica a la serie de tiempo
individuo_nomina = np.log(individuo["V8"].to_numpy())

# Definimos la cantidad de estados
num_states = 2

# Entrenamos el modelo
predicted_average, predicted_std, transition = hmm_tfp_batch(individuo_nomina, num_states)

# Graficamos la serie original, con las medias y desviación estándar de cada uno de los sistemas de los estados ocultos
fig, ax1 = plt.subplots(figsize=(10, 6))

fecha_plot = np.array([i[:7] for i in individuo.V6_new.to_numpy()]).astype('datetime64')

ax1.set_xlabel('Fecha [Mensual]')
ax1.set_ylabel("Pago de nómina\n[valores modificados por confidencialidad]", color='black')
ax1.plot(fecha_plot, individuo_nomina, color='black', label = "Pago realizado")
ax1.plot(fecha_plot, predicted_average, color="red", label = "Media estimada")
ax1.fill_between(fecha_plot, predicted_average+predicted_std, predicted_average-predicted_std, facecolor='orange', alpha=0.4, label = "Desviación estándar estimada")
plt.xticks(rotation=90)
plt.title(f"Modelo Oculto de Markov\nEstados Latentes : {num_states}")
plt.legend()
plt.legend(loc='upper left')
plt.show()

### Definimos 3 estados ocultos
num_states = 3

# Entrenamos el modelo
predicted_average, predicted_std, transition = hmm_tfp_batch(individuo_nomina, num_states)

# Graficamos la serie original, con las medias y desviación estándar de cada uno de los sistemas de los estados ocultos
fig, ax1 = plt.subplots(figsize=(10, 6))

fecha_plot = np.array([i[:7] for i in individuo.V6_new.to_numpy()]).astype('datetime64')

ax1.set_xlabel('Fecha [Mensual]')
ax1.set_ylabel("Pago de nómina\n[valores modificados por confidencialidad]", color='black')
ax1.plot(fecha_plot, individuo_nomina, color='black', label = "Pago realizado")
ax1.plot(fecha_plot, predicted_average, color="red", label = "Media estimada")
ax1.fill_between(fecha_plot, predicted_average+predicted_std, predicted_average-predicted_std, facecolor='orange', alpha=0.4, label = "Desviación estándar estimada")
plt.xticks(rotation=90)
plt.title(f"Modelo Oculto de Markov\nEstados Latentes : {num_states}")
plt.legend()
plt.legend(loc='upper left')
plt.show()
