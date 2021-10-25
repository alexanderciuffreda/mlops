# MLOPS GitHub repository
## useful commands
### useful kubernetes commands
get namespace name ips and ports
```
microk8s kubectl get services --all-namespaces
```

```
microk8s kubectl get all --all-namespaces
```
get pod id
```
microk8s kubectl get pods -n <namespace> <pod-name> -o jsonpath='{.metadata.uid}'
```
get all pod names from namespace kubeflow
```
microk8s kubectl get pods -n kubeflow
```
get pod id by pod name
```
microk8s kubectl get pods -n kubeflow oidc-gatekeeper-66786cf8d5-mvc2f -o jsonpath='{.metadata.uid}'
```
delete pod
```
microk8s.kubectl delete pod -n kubeflow <pod name>
```
### useful docs
https://microk8s.io/docs/addon-kubeflow

### useful gcloud commands
create new vm 6vCPUs 20 GB RAM 100 GB SSD
```
gcloud compute instances create kubeflow-01
```
connect to instance with ssh flag for SOCKS proxy
```
gcloud beta compute ssh --zone "us-west4-b" "kubeflow-01"  --project "mlops" --ssh-flag="-D9999"
```
### microk8s
install microk8s with channel specification
```
sudo snap install microk8s --classic --channel=1.21/stable
```
set permissions
```
sudo usermod -a -G microk8s $USER && sudo chown -f -R $USER ~/.kube
```
### GUI & VNC server
```
sudo apt-get -y update && sudo apt-get -y upgrade && sudo apt -y install && sudo apt -y install ubuntu-mate-core && sudo apt install tightvncserver && tightvncserver
```
### useful jupyter-book
update table
```
jupyter-book build mynewbook/
```

```
ghp-import -n -p -f _build/html 
```
